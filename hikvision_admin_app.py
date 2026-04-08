#!/usr/bin/env python3
from __future__ import annotations

import argparse
import atexit
import datetime as dt
import email.parser
import email.policy
import html
import io
import ipaddress
import json
import logging
import posixpath
import re
import shutil
import sqlite3
import threading
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlencode

from jinja2 import Environment, FileSystemLoader, select_autoescape

from admin_common import (
    APP_NAME,
    COOKIE_NAME,
    EMPLOYEE_STATE_ACTIVE,
    EMPLOYEE_STATE_DEACTIVATED,
    MAX_REQUEST_BYTES,
    PANEL_DEFAULT_NTP_INTERVAL,
    PANEL_DEFAULT_NTP_PORT,
    PANEL_DEFAULT_TIMEZONE,
    PANEL_DEFAULT_TIME_FORMAT,
    PANEL_DEFAULT_TIME_MODE,
    PANEL_TIMEZONE_CHOICES,
    ConfigManager,
    SessionManager,
    employee_display_name,
    employee_is_active,
    employee_state,
    ensure_dir,
    generate_password_hash,
    is_recent_iso,
    normalize_employee_state,
    setup_logging,
    to_bool,
    verify_password,
)
from admin_db import Database, row_to_panel_config
from admin_sync import (
    CleanupWorker,
    EventSupervisor,
    HikvisionSyncService,
    PanelSyncCacheWorker,
    preview_item_to_cache_payload,
)
from hikvision_multi_panel import (
    ERR_DEVICE_USER_ALREADY_EXIST_FACE,
    ERR_METHOD_NOT_ALLOWED,
    ERR_NOT_SUPPORT,
    HikvisionApiError,
    HikvisionISAPIClient,
)

logger = logging.getLogger(__name__)


@dataclass
class _UploadedFile:
    """Minimal file-upload wrapper replacing deprecated cgi.FieldStorage."""

    filename: str
    file: io.BytesIO


@dataclass
class RequestContext:
    environ: dict[str, Any]
    user: str | None
    form: dict[str, Any]
    files: dict[str, Any]


@dataclass
class AppRuntime:
    config: ConfigManager
    db: Database
    app: "AdminApp"
    supervisor: EventSupervisor
    cleanup: CleanupWorker
    _stopped: bool = False

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self.supervisor.stop()
        self.cleanup.stop()
        self.supervisor.join(timeout=5)
        self.cleanup.join(timeout=5)
        self.db.close()
        logger.info("Application runtime stopped")


class AdminApp:
    def __init__(self, config: ConfigManager, db: Database):
        self.config = config
        self.db = db
        self.session_manager = SessionManager(config)
        self.sync_service = HikvisionSyncService(
            db,
            config.resolve_path("storage", "employee_media_dir", "media/employees"),
        )
        self.panel_sync_refresher = PanelSyncCacheWorker(db, config, sync_service=self.sync_service)
        self._panel_refresh_lock = threading.Lock()
        self._panel_refresh_running = False
        self._panel_refresh_thread: threading.Thread | None = None
        self.templates = Environment(
            loader=FileSystemLoader(str(Path(__file__).parent / "templates")),
            autoescape=select_autoescape(["html", "xml"]),
        )
        self.templates.globals["app_name"] = APP_NAME
        self.templates.globals["human_result"] = self.human_result
        self.templates.globals["human_method"] = self.human_method
        self.templates.globals["panel_sync_totals"] = self.panel_sync_totals
        self.templates.globals["human_sync_action"] = self.human_sync_action
        self.templates.globals["human_sync_status"] = self.human_sync_status
        self.templates.globals["human_hik_error"] = self.human_hik_error
        self.templates.globals["human_panel_timezone"] = self.human_panel_timezone
        self.templates.globals["human_employee_state"] = self.human_employee_state
        self.templates.globals["employee_state_pill_class"] = self.employee_state_pill_class
        self.templates.globals["employee_toggle_label"] = self.employee_toggle_label
        self.templates.globals["employee_display_name"] = employee_display_name
        self.templates.globals["employee_sort_url"] = self.employee_sort_url
        self.templates.filters["format_dt"] = self._format_dt

    @staticmethod
    def _format_dt(value: str, fmt: str = "%d.%m.%Y %H:%M:%S") -> str:
        if not value:
            return "—"
        try:
            return dt.datetime.fromisoformat(str(value)).strftime(fmt)
        except (ValueError, TypeError):
            return str(value)

    def build_request_context(self, environ) -> RequestContext:
        cookie_header = environ.get("HTTP_COOKIE", "")
        cookies = self._parse_cookie_header(cookie_header)
        current_user = self.session_manager.parse_cookie_value(cookies.get(COOKIE_NAME))
        form, files = self._parse_request_data(environ)
        return RequestContext(environ=environ, user=current_user, form=form, files=files)

    def handle_public_route(self, path: str, method: str, ctx: RequestContext, start_response):
        if path.startswith("/static/"):
            return self.serve_static(path, start_response)
        if path.startswith("/media/"):
            return self.serve_media(path, start_response)
        if path == "/login":
            if method == "POST":
                return self.handle_login(ctx, start_response)
            return self.render(start_response, "login.html", {"title": "Вход", "ctx": ctx})
        if path == "/logout":
            return self.logout(start_response)
        return None

    def authenticated_routes(self) -> list[tuple[str, Any]]:
        return [
            ("/", self.dashboard),
            ("/employees", self.employees),
            ("/employees/new", self.employee_new),
            ("/employees/bulk", self.employee_bulk),
            ("/panels", self.panels),
            ("/panels/new", self.panel_new),
            ("/journal", self.journal),
            ("/settings", self.settings),
            ("/sync", self.sync_page),
            ("/sync/runs", self.sync_runs),
            ("/discrepancies", self.discrepancies),
            ("/sync/preview", self.sync_preview),
            ("/sync/apply", self.sync_apply),
            ("/cleanup/run", self.cleanup_run),
        ]

    def dispatch_authenticated_route(self, path: str, ctx: RequestContext, start_response):
        for route, handler in self.authenticated_routes():
            if path == route:
                return handler(ctx, start_response)
        if path.startswith("/employees/"):
            return self.employee_routes(path, ctx, start_response)
        if path.startswith("/panels/"):
            return self.panel_routes(path, ctx, start_response)
        if path.startswith("/journal/"):
            return self.journal_detail(path, ctx, start_response)
        start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"Not Found"]

    def __call__(self, environ, start_response):
        path = environ.get("PATH_INFO", "/")
        method = environ.get("REQUEST_METHOD", "GET").upper()
        try:
            ctx = self.build_request_context(environ)

            public_response = self.handle_public_route(path, method, ctx, start_response)
            if public_response is not None:
                return public_response
            if ctx.user is None:
                return self.redirect(start_response, "/login")
            if method == "GET":
                self.trigger_panel_cache_refresh(wait_seconds=0.35)
            return self.dispatch_authenticated_route(path, ctx, start_response)
        except Exception:  # noqa: BLE001
            logger.exception("Unhandled request error: %s %s", method, path)
            start_response("500 Internal Server Error", [("Content-Type", "text/plain; charset=utf-8")])
            return ["Internal Server Error".encode("utf-8")]

    def trigger_panel_cache_refresh(self, wait_seconds: float = 0.0) -> None:
        thread: threading.Thread | None = None
        with self._panel_refresh_lock:
            if self._panel_refresh_running:
                thread = self._panel_refresh_thread
            else:
                self._panel_refresh_running = True

                def runner() -> None:
                    try:
                        self.panel_sync_refresher.refresh_once()
                    except Exception:  # noqa: BLE001
                        logger.exception("On-demand panel cache refresh failed")
                    finally:
                        with self._panel_refresh_lock:
                            self._panel_refresh_running = False
                            self._panel_refresh_thread = None

                thread = threading.Thread(target=runner, daemon=True, name="panel-cache-refresh-on-demand")
                self._panel_refresh_thread = thread
                thread.start()
        if thread and wait_seconds > 0:
            thread.join(wait_seconds)

    def _parse_request_data(self, environ) -> tuple[dict[str, Any], dict[str, Any]]:
        method = environ.get("REQUEST_METHOD", "GET").upper()
        if method not in {"POST", "PUT"}:
            return {}, {}
        content_type = environ.get("CONTENT_TYPE", "")
        if "multipart/form-data" in content_type:
            length = int(environ.get("CONTENT_LENGTH") or "0")
            if length > MAX_REQUEST_BYTES:
                return {}, {}
            raw_body = environ["wsgi.input"].read(length) if length else b""
            # Reconstruct a minimal MIME message so email.parser can handle multipart
            fake_header = f"Content-Type: {content_type}\r\n\r\n".encode("utf-8")
            msg = email.parser.BytesParser(policy=email.policy.compat32).parsebytes(fake_header + raw_body)
            form: dict[str, Any] = {}
            files: dict[str, Any] = {}
            payload = msg.get_payload()
            if isinstance(payload, list):
                for part in payload:
                    disposition = part.get("Content-Disposition", "")
                    params: dict[str, str] = {}
                    for segment in disposition.split(";"):
                        segment = segment.strip()
                        if "=" in segment:
                            k, _, v = segment.partition("=")
                            params[k.strip().lower()] = v.strip(' "')
                    name = params.get("name")
                    if not name:
                        continue
                    filename = params.get("filename")
                    data: bytes = part.get_payload(decode=True) or b""
                    if filename:
                        files[name] = _UploadedFile(filename=filename, file=io.BytesIO(data))
                    else:
                        form[name] = data.decode("utf-8", errors="replace")
            return form, files
        length = int(environ.get("CONTENT_LENGTH") or "0")
        if length > MAX_REQUEST_BYTES:
            return {}, {}
        raw_body = environ["wsgi.input"].read(length) if length else b""
        form = {k: v[-1] for k, v in parse_qs(raw_body.decode("utf-8"), keep_blank_values=True).items()}
        return form, {}

    def _parse_cookie_header(self, cookie_header: str) -> dict[str, str]:
        result = {}
        for chunk in cookie_header.split(";"):
            if "=" not in chunk:
                continue
            key, value = chunk.strip().split("=", 1)
            result[key] = value
        return result

    def render(self, start_response, template_name: str, context: dict[str, Any], status: str = "200 OK"):
        headers = [("Content-Type", "text/html; charset=utf-8")]
        ctx = context.get("ctx")
        flash = self.consume_flash(ctx) if ctx else None
        context = dict(context)
        context["flash"] = flash
        template = self.templates.get_template(template_name)
        payload = template.render(**context).encode("utf-8")
        if flash:
            headers.append(("Set-Cookie", "flash=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"))
        start_response(status, headers)
        return [payload]

    def redirect(self, start_response, location: str, headers: list[tuple[str, str]] | None = None):
        final_headers = [("Location", location)]
        if headers:
            final_headers.extend(headers)
        start_response("302 Found", final_headers)
        return [b""]

    def flash_redirect(self, start_response, location: str, message: str):
        encoded_message = quote(message, safe="")
        return self.redirect(
            start_response,
            location,
            headers=[("Set-Cookie", f"flash={encoded_message}; Path=/; HttpOnly; SameSite=Lax")],
        )

    def consume_flash(self, ctx: RequestContext) -> str | None:
        cookies = self._parse_cookie_header(ctx.environ.get("HTTP_COOKIE", ""))
        value = cookies.get("flash")
        return unquote(value) if value else None

    def handle_login(self, ctx: RequestContext, start_response):
        username = ctx.form.get("username", "").strip()
        password = ctx.form.get("password", "")
        for user in self.config.auth_users:
            if user.get("username") == username and to_bool(user.get("is_active", True)):
                if verify_password(password, str(user.get("password_hash", ""))):
                    cookie = self.session_manager.create_cookie_value(username)
                    secure_flag = "; Secure" if self.config.session_cookie_secure else ""
                    return self.redirect(
                        start_response,
                        "/",
                        headers=[(
                            "Set-Cookie",
                            f"{COOKIE_NAME}={cookie}; Path=/; HttpOnly; SameSite=Lax{secure_flag}",
                        )],
                    )
        return self.render(
            start_response,
            "login.html",
            {"title": "Вход", "ctx": ctx, "error": "Неверный логин или пароль"},
        )

    def logout(self, start_response):
        secure_flag = "; Secure" if self.config.session_cookie_secure else ""
        return self.redirect(
            start_response,
            "/login",
            headers=[(
                "Set-Cookie",
                f"{COOKIE_NAME}=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax{secure_flag}",
            )],
        )

    def dashboard(self, ctx: RequestContext, start_response):
        return self.render(start_response, "dashboard.html", self.dashboard_context(ctx))

    def employee_sort_state(self, ctx: RequestContext) -> tuple[str, str]:
        sort_by = (ctx.form.get("sort", "") or self._query_param(ctx, "sort", "employee_id")).strip() or "employee_id"
        if sort_by not in {"name", "employee_id", "card_number", "status"}:
            sort_by = "employee_id"
        sort_dir = (ctx.form.get("dir", "") or self._query_param(ctx, "dir", "desc")).strip().lower() or "desc"
        if sort_dir not in {"asc", "desc"}:
            sort_dir = "desc"
        return sort_by, sort_dir

    def employee_sort_url(
        self,
        target_sort: str,
        current_sort: str,
        current_dir: str,
        include_inactive: bool,
        search: str,
        view_mode: str,
    ) -> str:
        next_dir = "desc" if current_sort == target_sort and current_dir == "asc" else "asc"
        params = {
            "sort": target_sort,
            "dir": next_dir,
            "include_inactive": "1" if include_inactive else "0",
            "view": view_mode or "table",
        }
        if search:
            params["q"] = search
        return f"/employees?{urlencode(params)}"

    @staticmethod
    def _normalize_optional_text(value: Any) -> str:
        text = str(value or "").strip()
        if text.lower() in {"none", "null"}:
            return ""
        return text

    def employees(self, ctx: RequestContext, start_response):
        include_inactive = to_bool(ctx.form.get("include_inactive") or self._query_param(ctx, "include_inactive", "1"))
        search = self._query_param(ctx, "q", "")
        view_mode = self._query_param(ctx, "view", "table")
        sort_by, sort_dir = self.employee_sort_state(ctx)
        employees = self.db.list_employees(
            include_inactive=include_inactive,
            search=search,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        panels = self.db.list_panels(enabled_only=True)
        sync_status = self.build_employee_sync_status(employees, panels)
        return self.render(
            start_response,
            "employees.html",
            {
                "title": "Сотрудники",
                "ctx": ctx,
                "employees": employees,
                "panels": panels,
                "sync_status": sync_status,
                "include_inactive": include_inactive,
                "search": search,
                "view_mode": view_mode,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
            },
        )

    def employee_new(self, ctx: RequestContext, start_response):
        if ctx.environ.get("REQUEST_METHOD") == "POST":
            return self.save_employee(None, ctx, start_response)
        return self.render_employee_form(start_response, ctx, employee=None, employee_pk=None, title="Новый сотрудник")

    def parse_resource_route(self, path: str, resource: str) -> tuple[int | None, str | None]:
        parts = path.strip("/").split("/")
        if len(parts) < 2 or parts[0] != resource or not parts[1].isdigit():
            return None, None
        return int(parts[1]), parts[2] if len(parts) > 2 else None

    def employee_routes(self, path: str, ctx: RequestContext, start_response):
        employee_pk, action = self.parse_resource_route(path, "employees")
        if employee_pk is None:
            return self.redirect(start_response, "/employees")
        if action == "edit":
            if ctx.environ.get("REQUEST_METHOD") == "POST":
                return self.save_employee(employee_pk, ctx, start_response)
            employee = self.db.get_employee(employee_pk)
            return self.render_employee_form(
                start_response,
                ctx,
                employee=employee,
                employee_pk=employee_pk,
                title="Редактировать сотрудника",
            )
        if action == "sync":
            employee = self.db.get_employee(employee_pk)
            if employee:
                return self.redirect(start_response, f"/sync?employee_id={employee['employee_id']}")
            return self.redirect(start_response, "/employees")
        if action == "toggle" and ctx.environ.get("REQUEST_METHOD") == "POST":
            employee = self.db.get_employee(employee_pk)
            if employee:
                current_state = employee_state(employee)
                target_state = EMPLOYEE_STATE_DEACTIVATED if current_state == EMPLOYEE_STATE_ACTIVE else EMPLOYEE_STATE_ACTIVE
                self.db.set_employee_state(employee_pk, target_state)
            return self.redirect(start_response, self.employee_return_url(ctx, force_include_inactive=True))
        if action == "delete" and ctx.environ.get("REQUEST_METHOD") == "POST":
            employee = self.db.get_employee(employee_pk)
            if employee:
                self.db.delete_employee(employee_pk)
                self.delete_employee_photo_file(employee["photo_path"])
                return self.flash_redirect(
                    start_response,
                    self.employee_return_url(ctx, force_include_inactive=True),
                    "Сотрудник удалён из локальной БД. При следующей синхронизации запись будет удалена с панелей.",
                )
            return self.redirect(start_response, self.employee_return_url(ctx, force_include_inactive=True))
        if action == "photo-delete" and ctx.environ.get("REQUEST_METHOD") == "POST":
            employee = self.db.get_employee(employee_pk)
            if employee and employee["photo_path"]:
                self.db.update_employee_photo(employee_pk, None)
                self.delete_employee_photo_file(employee["photo_path"])
                return self.flash_redirect(
                    start_response,
                    f"/employees/{employee_pk}/edit",
                    "Фото сотрудника удалено.",
                )
            return self.redirect(start_response, f"/employees/{employee_pk}/edit")
        return self.redirect(start_response, "/employees")

    def employee_return_url(self, ctx: RequestContext, force_include_inactive: bool = False) -> str:
        view_mode = (ctx.form.get("current_view", "") or self._query_param(ctx, "view", "")).strip() or "table"
        search = (ctx.form.get("current_search", "") or self._query_param(ctx, "q", "")).strip()
        include_inactive = force_include_inactive or to_bool(
            ctx.form.get("current_include_inactive", "") or self._query_param(ctx, "include_inactive", "1")
        )
        sort_by, sort_dir = self.employee_sort_state(ctx)
        params = {
            "view": view_mode,
            "include_inactive": "1" if include_inactive else "0",
            "sort": sort_by,
            "dir": sort_dir,
        }
        if search:
            params["q"] = search
        return f"/employees?{urlencode(params)}"

    def save_employee(self, employee_pk: int | None, ctx: RequestContext, start_response):
        current = self.db.get_employee(employee_pk) if employee_pk else None
        full_name = self._normalize_optional_text(ctx.form.get("full_name", ""))
        values = {
            "employee_id": ctx.form.get("employee_id", "").strip(),
            "full_name": full_name,
            "first_name": full_name,
            "last_name": "",
            "room_number": self._normalize_optional_text(ctx.form.get("room_number", "")) or "1",
            "card_number": self._normalize_optional_text(ctx.form.get("card_number", "")),
            "comment": ctx.form.get("comment", "").strip(),
            "lifecycle_state": normalize_employee_state(ctx.form.get("lifecycle_state", EMPLOYEE_STATE_ACTIVE)),
            "photo_path": current["photo_path"] if current else None,
        }
        error = self.validate_employee(values, employee_pk=employee_pk)
        if error:
            return self.render_employee_form(
                start_response,
                ctx,
                employee=values,
                employee_pk=employee_pk,
                error=error,
                title="Редактировать сотрудника" if employee_pk else "Новый сотрудник",
                status_code="400 Bad Request",
            )
        upload = ctx.files.get("photo")
        previous_photo_path = str(current["photo_path"]) if current and current["photo_path"] else None
        uploaded_photo_path: str | None = None
        if upload is not None and getattr(upload, "filename", ""):
            uploaded_photo_path = self.save_employee_photo(upload)
            values["photo_path"] = uploaded_photo_path
        try:
            saved_id = self.db.save_employee(values, employee_pk=employee_pk)
        except sqlite3.IntegrityError:
            if uploaded_photo_path:
                self.delete_employee_photo_file(uploaded_photo_path)
            return self.render_employee_form(
                start_response,
                ctx,
                employee=values,
                employee_pk=employee_pk,
                error="Сотрудник с таким ID или номером карты уже существует.",
                title="Редактировать сотрудника" if employee_pk else "Новый сотрудник",
                status_code="400 Bad Request",
            )
        if previous_photo_path and values.get("photo_path") and values["photo_path"] != previous_photo_path:
            self.delete_employee_photo_file(previous_photo_path)
        return self.flash_redirect(
            start_response,
            f"/employees/{saved_id}/edit",
            "Сотрудник сохранён. При необходимости можно сразу синхронизировать его с панелями.",
        )

    def validate_employee(self, values: dict[str, Any], employee_pk: int | None = None) -> str | None:
        if not values["employee_id"]:
            return "Укажи идентификатор сотрудника."
        if not values.get("full_name"):
            return "Укажи имя сотрудника."
        if len(str(values["full_name"])) > 32:
            return "Имя сотрудника на панели не должно быть длиннее 32 символов."
        existing = self.db.get_employee_by_employee_id(values["employee_id"])
        if existing and int(existing["id"]) != int(employee_pk or 0):
            return "Сотрудник с таким идентификатором уже существует."
        if values.get("card_number"):
            card_owner = self.db.get_employee_by_card(values["card_number"])
            if card_owner and int(card_owner["id"]) != int(employee_pk or 0):
                return "Этот номер карты уже привязан к другому сотруднику."
        return None

    def employee_form_context(
        self,
        ctx: RequestContext,
        employee: dict[str, Any] | sqlite3.Row | None,
        employee_pk: int | None,
        title: str,
        error: str | None = None,
    ) -> dict[str, Any]:
        events = self.db.latest_employee_events(employee_pk) if employee_pk else []
        employee_sync_id = employee["employee_id"] if employee and "employee_id" in employee.keys() else None
        employee_sync_actions = self.db.employee_sync_actions(employee_sync_id) if employee_sync_id else []
        return {
            "title": title,
            "ctx": ctx,
            "employee": employee,
            "events": events,
            "panels": [],
            "sync_status": {},
            "employee_sync_actions": employee_sync_actions,
            "error": error,
        }

    def render_employee_form(
        self,
        start_response,
        ctx: RequestContext,
        employee: dict[str, Any] | sqlite3.Row | None,
        employee_pk: int | None,
        title: str,
        error: str | None = None,
        status_code: str = "200 OK",
    ):
        return self.render(
            start_response,
            "employee_form.html",
            self.employee_form_context(ctx, employee, employee_pk, title, error=error),
            status=status_code,
        )

    def parse_sync_selection(self, ctx: RequestContext) -> tuple[list[int], list[str]]:
        panel_ids = [int(value) for value in ctx.form.get("panel_ids", "").split(",") if value.strip().isdigit()]
        employee_ids = [value for value in ctx.form.get("employee_ids", "").split(",") if value.strip()]
        return panel_ids, employee_ids

    def panel_timezone_choices(self, current_value: str | None = None) -> list[dict[str, str]]:
        current = (current_value or "").strip()
        choices = [dict(item) for item in PANEL_TIMEZONE_CHOICES]
        if current and all(item["value"] != current for item in choices):
            choices.append({"value": current, "label": self.human_panel_timezone(current)})
        return choices

    def human_panel_timezone(self, value: str | None) -> str:
        raw = (value or "").strip()
        if not raw:
            return "—"
        for item in PANEL_TIMEZONE_CHOICES:
            if item["value"] == raw:
                return item["label"]
        match = re.fullmatch(r"CST([+-])(\d{1,2}):(\d{2}):(\d{2})", raw)
        if not match:
            return raw
        sign, hours, minutes, _seconds = match.groups()
        utc_sign = "+" if sign == "-" else "-"
        return f"UTC{utc_sign}{int(hours):02d}:{int(minutes):02d}"

    def timezone_from_panel_value(self, value: str | None) -> dt.timezone:
        raw = (value or PANEL_DEFAULT_TIMEZONE).strip() or PANEL_DEFAULT_TIMEZONE
        match = re.fullmatch(r"CST([+-])(\d{1,2}):(\d{2}):(\d{2})", raw)
        if not match:
            return dt.timezone(dt.timedelta(hours=3))
        sign, hours, minutes, seconds = match.groups()
        delta = dt.timedelta(hours=int(hours), minutes=int(minutes), seconds=int(seconds))
        if sign == "+":
            delta = -delta
        return dt.timezone(delta)

    def human_employee_state(self, value: Any) -> str:
        state = employee_state(value)
        mapping = {
            EMPLOYEE_STATE_ACTIVE: "активен",
            EMPLOYEE_STATE_DEACTIVATED: "деактивирован",
        }
        return mapping.get(state, "деактивирован")

    def employee_state_pill_class(self, value: Any) -> str:
        state = employee_state(value)
        mapping = {
            EMPLOYEE_STATE_ACTIVE: "ok",
            EMPLOYEE_STATE_DEACTIVATED: "warn",
        }
        return mapping.get(state, "warn")

    def employee_toggle_label(self, value: Any) -> str:
        state = employee_state(value)
        if state == EMPLOYEE_STATE_ACTIVE:
            return "Деактивировать"
        return "Активировать"

    def normalize_manual_time(self, raw_value: str | None, time_zone: str | None) -> str:
        value = (raw_value or "").strip()
        if not value:
            return ""
        try:
            parsed = dt.datetime.fromisoformat(value)
        except ValueError:
            return value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=self.timezone_from_panel_value(time_zone))
        return parsed.isoformat(timespec="seconds")

    def sync_run_summary(self, sync_run: sqlite3.Row | dict[str, Any] | None) -> dict[str, int]:
        defaults = {"created": 0, "updated": 0, "deleted": 0, "errors": 0}
        if not sync_run:
            return defaults
        if isinstance(sync_run, sqlite3.Row):
            raw_summary = sync_run["summary_json"]
        else:
            raw_summary = sync_run.get("summary_json")
        if not raw_summary:
            return defaults
        try:
            data = json.loads(raw_summary)
        except (TypeError, ValueError, json.JSONDecodeError):
            return defaults
        return {
            "created": int(data.get("created", 0) or 0),
            "updated": int(data.get("updated", 0) or 0),
            "deleted": int(data.get("deleted", 0) or 0),
            "errors": int(data.get("errors", 0) or 0),
        }

    def sync_run_card(self, sync_run: sqlite3.Row | dict[str, Any] | None, run_actions: list[sqlite3.Row] | None = None) -> dict[str, Any] | None:
        if not sync_run:
            return None
        sync_run_id = int(sync_run["id"] if isinstance(sync_run, sqlite3.Row) else sync_run.get("id", 0) or 0)
        if not sync_run_id:
            return None
        summary = self.sync_run_summary(sync_run)
        actions = run_actions if run_actions is not None else self.db.list_sync_actions(sync_run_id)
        tone = "success" if summary.get("errors", 0) == 0 else "warn"
        return {
            "id": sync_run_id,
            "status": sync_run["status"] if isinstance(sync_run, sqlite3.Row) else sync_run.get("status", ""),
            "started_at": sync_run["started_at"] if isinstance(sync_run, sqlite3.Row) else sync_run.get("started_at"),
            "finished_at": sync_run["finished_at"] if isinstance(sync_run, sqlite3.Row) else sync_run.get("finished_at"),
            "summary": summary,
            "actions": actions,
            "tone": tone,
        }

    def sync_page_context(
        self,
        ctx: RequestContext,
        panel_ids: list[int] | None = None,
        employee_ids: list[str] | None = None,
        preview: dict[str, Any] | None = None,
        sync_result: dict[str, Any] | None = None,
        run_actions: list[sqlite3.Row] | None = None,
    ) -> dict[str, Any]:
        last_sync = None
        if sync_result:
            run_row = self.db.get_sync_run(int(sync_result["run_id"])) if sync_result.get("run_id") else None
            if run_row is None:
                run_row = {
                    "id": sync_result.get("run_id"),
                    "status": "finished",
                    "started_at": None,
                    "finished_at": None,
                    "summary_json": json.dumps(sync_result.get("summary", {}), ensure_ascii=False),
                }
            last_sync = self.sync_run_card(run_row, run_actions=run_actions or [])
        preview_has_deletes = any(item.get("deletes") for item in (preview or {}).get("panels", [])) if preview else False
        return {
            "title": "Синхронизация",
            "ctx": ctx,
            "employees": self.db.list_employees(include_inactive=False),
            "panels": self.db.list_panels(enabled_only=True),
            "preview": preview,
            "sync_result": sync_result,
            "run_actions": run_actions or [],
            "last_sync": last_sync,
            "selected_panel_ids": panel_ids or [],
            "selected_employee_ids": employee_ids or [],
            "preview_has_deletes": preview_has_deletes,
        }

    _ALLOWED_PHOTO_EXTENSIONS = {".jpg", ".jpeg", ".png"}

    def save_employee_photo(self, upload) -> str:
        employee_media_dir = self.config.resolve_path("storage", "employee_media_dir", "media/employees")
        ensure_dir(employee_media_dir)
        suffix = Path(upload.filename).suffix.lower()
        if suffix not in self._ALLOWED_PHOTO_EXTENSIONS:
            suffix = ".jpg"
        filename = f"{uuid.uuid4().hex}{suffix}"
        target = employee_media_dir / filename
        with target.open("wb") as fh:
            shutil.copyfileobj(upload.file, fh)
        return filename

    def delete_employee_photo_file(self, photo_path: str | None) -> None:
        if not photo_path:
            return
        employee_media_dir = self.config.resolve_path("storage", "employee_media_dir", "media/employees")
        target = employee_media_dir / str(photo_path)
        try:
            if target.exists() and target.is_file():
                target.unlink()
        except OSError:
            pass

    def dashboard_context(self, ctx: RequestContext) -> dict[str, Any]:
        stats = self.db.get_dashboard_stats()
        latest_events = self.db.list_journal({"show_unmatched": True}, limit=10)
        latest_syncs = self.db.latest_sync_runs(limit=10)
        latest_sync_errors = self.db.latest_sync_errors(limit=8)
        panel_states = self.db.list_panel_health_status()
        today = dt.date.today().isoformat()
        today_events = self.db.list_journal(
            {"show_unmatched": True, "date_from": today, "date_to": today},
            limit=8,
        )
        employees = self.db.list_employees(include_inactive=False)
        panels = self.db.list_panels(enabled_only=True)
        sync_status = self.build_employee_sync_status(employees, panels)
        unsynced_employees = [
            employee
            for employee in employees
            if any(
                item["icon"] in {"cloud_off", "sync_problem"}
                for item in sync_status.get(employee["id"], {}).values()
            )
        ]
        return {
            "title": "Панель управления",
            "ctx": ctx,
            "stats": stats,
            "events": latest_events,
            "sync_runs": latest_syncs,
            "panel_states": panel_states,
            "sync_errors": latest_sync_errors,
            "today_events": today_events,
            "unsynced_employees": unsynced_employees[:8],
            "panels": panels,
            "sync_status": sync_status,
        }

    def sync_cache_interval_minutes(self) -> int:
        return int(
            self.config.get("server", "panel_sync_interval_minutes", self.config.get("server", "panel_health_interval_minutes", 5))
            or 5
        )

    def panel_sync_cache_by_id(self) -> dict[int, sqlite3.Row]:
        return {int(row["panel_id"]): row for row in self.db.list_panel_sync_cache()}

    def panel_sync_cache_is_fresh(self, cache_row: sqlite3.Row | None) -> bool:
        if not cache_row or str(cache_row["state"]) not in {"ok", "error"}:
            return False
        max_age = dt.timedelta(minutes=max(2, self.sync_cache_interval_minutes() * 2))
        return is_recent_iso(str(cache_row["updated_at"] or ""), max_age)

    def cached_panel_preview(self, panel_id: int) -> dict[str, Any] | None:
        cache_row = self.panel_sync_cache_by_id().get(panel_id)
        if not self.panel_sync_cache_is_fresh(cache_row):
            return None
        payload_json = str(cache_row["payload_json"] or "").strip() if cache_row else ""
        if not payload_json:
            return None
        try:
            return json.loads(payload_json)
        except json.JSONDecodeError:
            return None

    def store_sync_cache(self, preview: dict[str, Any]) -> None:
        for item in preview.get("panels", []):
            panel_obj = item.get("panel") or {}
            panel = panel_obj if isinstance(panel_obj, dict) else dict(panel_obj)
            panel_id = int(panel.get("id") or 0)
            if not panel_id:
                continue
            self.db.update_panel_sync_cache(
                panel_id,
                "error" if item.get("error") else "ok",
                str(item.get("error") or ""),
                preview_item_to_cache_payload(item),
            )

    def refresh_sync_cache(self, panel_ids: list[int] | None = None) -> None:
        preview = self.sync_service.sync_preview(panel_ids=panel_ids or None)
        self.store_sync_cache(preview)

    def panel_detail_context(self, ctx: RequestContext, panel_pk: int) -> dict[str, Any]:
        panel = self.db.get_panel(panel_pk)
        panel_status = next(
            (row for row in self.db.list_panel_health_status() if int(row["panel_id"]) == panel_pk),
            None,
        )
        preview_item = self.cached_panel_preview(panel_pk) if panel and self.panel_is_available_for_ui(panel_status) else None
        preview = {"panels": [preview_item]} if preview_item else None
        events = self.db.list_journal({"panel_id": panel_pk, "show_unmatched": True}, limit=20)
        employees_total = len(self.db.list_employees(include_inactive=False))
        panel_summary = None
        if preview and preview["panels"]:
            current = preview["panels"][0]
            attention = len(current.get("creates", [])) + len(current.get("updates", []))
            panel_summary = {
                "employees_total": employees_total,
                "needs_attention": attention,
                "synced": max(0, employees_total - attention),
            }
        return {
            "title": f"Панель {panel['name']}" if panel else "Панель",
            "ctx": ctx,
            "panel": panel,
            "preview": preview,
            "events": events,
            "panel_status": panel_status,
            "panel_summary": panel_summary,
        }

    def filtered_sync_actions(
        self,
        sync_run_id: int | None,
        panel_filter: int | None = None,
        employee_filter: str = "",
        status_filter: str = "",
    ) -> list[sqlite3.Row]:
        actions = self.db.list_sync_actions(sync_run_id) if sync_run_id else []
        if panel_filter:
            actions = [row for row in actions if int(row["panel_id"]) == panel_filter]
        if employee_filter:
            actions = [row for row in actions if str(row["employee_id"] or "") == employee_filter]
        if status_filter:
            actions = [row for row in actions if str(row["status"]) == status_filter]
        return actions

    def employee_bulk(self, ctx: RequestContext, start_response):
        if ctx.environ.get("REQUEST_METHOD") != "POST":
            return self.redirect(start_response, "/employees")
        employee_ids = [value for value in ctx.form.get("selected_employee_ids", "").split(",") if value.strip().isdigit()]
        action = ctx.form.get("bulk_action", "").strip()
        if not employee_ids:
            return self.flash_redirect(start_response, self.employee_return_url(ctx), "Отметь хотя бы одного сотрудника.")
        selected = [self.db.get_employee(int(employee_pk)) for employee_pk in employee_ids]
        selected = [row for row in selected if row]
        if action == "activate":
            for employee in selected:
                self.db.set_employee_state(int(employee["id"]), EMPLOYEE_STATE_ACTIVE)
            return self.flash_redirect(
                start_response,
                self.employee_return_url(ctx, force_include_inactive=True),
                "Выбранные сотрудники активированы.",
            )
        if action == "deactivate":
            for employee in selected:
                self.db.set_employee_state(int(employee["id"]), EMPLOYEE_STATE_DEACTIVATED)
            return self.flash_redirect(
                start_response,
                self.employee_return_url(ctx, force_include_inactive=True),
                "Выбранные сотрудники деактивированы и остались в списке как неактивные.",
            )
        if action == "delete":
            for employee in selected:
                self.db.delete_employee(int(employee["id"]))
                self.delete_employee_photo_file(employee["photo_path"])
            return self.flash_redirect(
                start_response,
                self.employee_return_url(ctx, force_include_inactive=True),
                "Выбранные сотрудники удалены из локальной БД. При следующей синхронизации они будут удалены с панелей.",
            )
        if action == "sync":
            employee_nos = ",".join(employee["employee_id"] for employee in selected)
            return self.redirect(start_response, f"/sync?employee_ids={employee_nos}")
        return self.redirect(start_response, self.employee_return_url(ctx))

    def panels(self, ctx: RequestContext, start_response):
        if ctx.environ.get("REQUEST_METHOD") == "POST":
            return self.apply_bulk_panel_settings(ctx, start_response)
        panels = self.db.list_panels()
        status_by_id = {row["panel_id"]: row for row in self.db.list_panel_health_status()}
        enabled_panels = [panel for panel in panels if to_bool(panel["enabled"])]
        bulk_source = enabled_panels[0] if enabled_panels else (panels[0] if panels else None)
        bulk_settings = {
            "time_zone": str(bulk_source["time_zone"]) if bulk_source else PANEL_DEFAULT_TIMEZONE,
            "time_display_format": str(bulk_source["time_display_format"]) if bulk_source else PANEL_DEFAULT_TIME_FORMAT,
            "time_mode": str(bulk_source["time_mode"]) if bulk_source else PANEL_DEFAULT_TIME_MODE,
            "manual_time": str(bulk_source["manual_time"]) if bulk_source and bulk_source["manual_time"] else "",
            "ntp_server": str(bulk_source["ntp_server"]) if bulk_source and bulk_source["ntp_server"] else "",
            "ntp_port": int(bulk_source["ntp_port"]) if bulk_source else 123,
            "ntp_interval": int(bulk_source["ntp_interval"]) if bulk_source else 60,
            "face_auth_enabled": 1 if (not bulk_source or to_bool(bulk_source["face_auth_enabled"])) else 0,
        }
        return self.render(
            start_response,
            "panels.html",
            {
                "title": "Панели",
                "ctx": ctx,
                "panels": panels,
                "status_by_id": status_by_id,
                "bulk_settings": bulk_settings,
                "enabled_panels_count": len(enabled_panels),
                "panel_timezone_choices": self.panel_timezone_choices(bulk_settings["time_zone"]),
            },
        )

    def panel_new(self, ctx: RequestContext, start_response):
        if ctx.environ.get("REQUEST_METHOD") == "POST":
            return self.save_panel(None, ctx, start_response)
        return self.render(
            start_response,
            "panel_form.html",
            {
                "title": "Новая панель",
                "ctx": ctx,
                "panel": None,
                "panel_timezone_choices": self.panel_timezone_choices(PANEL_DEFAULT_TIMEZONE),
            },
        )

    def check_panel_health(self, panel_pk: int) -> tuple[bool, str]:
        panel = self.db.get_panel(panel_pk)
        if not panel:
            return False, "Панель не найдена"
        try:
            users, error = self.sync_service.list_panel_users(panel)
            if error:
                self.db.update_panel_health_status(panel_pk, "error", error)
                return False, error
            message = f"Доступна, пользователей найдено: {len(users)}"
            self.db.update_panel_health_status(panel_pk, "healthy", message)
            logger.info("Panel health check OK for %s: %s", panel["name"], message)
            return True, message
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            self.db.update_panel_health_status(panel_pk, "error", message)
            logger.warning("Panel health check failed for %s: %s", panel["name"], message)
            return False, message

    def panel_routes(self, path: str, ctx: RequestContext, start_response):
        panel_pk, action = self.parse_resource_route(path, "panels")
        if panel_pk is None:
            return self.redirect(start_response, "/panels")
        if action is None and ctx.environ.get("REQUEST_METHOD") == "GET":
            return self.render(start_response, "panel_detail.html", self.panel_detail_context(ctx, panel_pk))
        if action == "check" and ctx.environ.get("REQUEST_METHOD") == "POST":
            ok, message = self.check_panel_health(panel_pk)
            referer = ctx.environ.get("HTTP_REFERER") or f"/panels/{panel_pk}"
            if ok:
                return self.flash_redirect(start_response, referer, f"Панель проверена: {message}")
            return self.flash_redirect(start_response, referer, f"Проверка панели завершилась ошибкой: {message}")
        if action == "edit":
            if ctx.environ.get("REQUEST_METHOD") == "POST":
                return self.save_panel(panel_pk, ctx, start_response)
            panel = self.db.get_panel(panel_pk)
            return self.render(
                start_response,
                "panel_form.html",
                {
                    "title": "Редактировать панель",
                    "ctx": ctx,
                    "panel": panel,
                    "panel_timezone_choices": self.panel_timezone_choices(
                        str(panel["time_zone"]) if panel and panel["time_zone"] else PANEL_DEFAULT_TIMEZONE
                    ),
                },
            )
        if action == "delete" and ctx.environ.get("REQUEST_METHOD") == "POST":
            self.db.delete_panel(panel_pk)
            return self.redirect(start_response, "/panels")
        return self.redirect(start_response, "/panels")

    def save_panel(self, panel_pk: int | None, ctx: RequestContext, start_response):
        values = {
            "name": ctx.form.get("name", "").strip(),
            "host": ctx.form.get("host", "").strip(),
            "username": ctx.form.get("username", "").strip(),
            "password": ctx.form.get("password", "").strip(),
            "enabled": ctx.form.get("enabled", "1"),
            "time_zone": ctx.form.get("time_zone", PANEL_DEFAULT_TIMEZONE).strip() or PANEL_DEFAULT_TIMEZONE,
            "time_display_format": ctx.form.get("time_display_format", PANEL_DEFAULT_TIME_FORMAT).strip() or PANEL_DEFAULT_TIME_FORMAT,
            "time_mode": ctx.form.get("time_mode", PANEL_DEFAULT_TIME_MODE).strip() or PANEL_DEFAULT_TIME_MODE,
            "manual_time": self.normalize_manual_time(
                ctx.form.get("manual_time", "").strip(),
                ctx.form.get("time_zone", PANEL_DEFAULT_TIMEZONE).strip() or PANEL_DEFAULT_TIMEZONE,
            ),
            "ntp_server": ctx.form.get("ntp_server", "").strip(),
            "ntp_port": ctx.form.get("ntp_port", str(PANEL_DEFAULT_NTP_PORT)).strip() or str(PANEL_DEFAULT_NTP_PORT),
            "ntp_interval": ctx.form.get("ntp_interval", str(PANEL_DEFAULT_NTP_INTERVAL)).strip() or str(PANEL_DEFAULT_NTP_INTERVAL),
            "face_auth_enabled": ctx.form.get("face_auth_enabled", "1"),
        }
        if panel_pk:
            existing = self.db.get_panel(panel_pk)
            if existing and not values["password"]:
                values["password"] = existing["password"]
        saved_id = self.db.save_panel(values, panel_pk=panel_pk)
        messages = ["Настройки панели сохранены."]
        logger.info("Panel settings saved: id=%s name=%s", saved_id, values["name"])
        try:
            messages.extend(self.apply_panel_time_settings(self.db.get_panel(saved_id)))
        except Exception as exc:  # noqa: BLE001
            messages.append(f"Часовой пояс не применён на устройство: {self.human_hik_error(str(exc))}")
            logger.exception("Failed to apply time settings for panel id=%s", saved_id)
        try:
            messages.extend(self.apply_panel_face_auth(self.db.get_panel(saved_id)))
        except Exception as exc:  # noqa: BLE001
            messages.append(f"Аутентификация лица не применена на устройство: {self.human_hik_error(str(exc))}")
            logger.exception("Failed to apply face auth settings for panel id=%s", saved_id)
        return self.flash_redirect(
            start_response,
            f"/panels/{saved_id}",
            " ".join(filter(None, messages)),
        )

    def bulk_panel_values(self, ctx: RequestContext) -> dict[str, Any]:
        time_zone = ctx.form.get("time_zone", PANEL_DEFAULT_TIMEZONE).strip() or PANEL_DEFAULT_TIMEZONE
        return {
            "time_zone": time_zone,
            "time_display_format": ctx.form.get("time_display_format", PANEL_DEFAULT_TIME_FORMAT).strip() or PANEL_DEFAULT_TIME_FORMAT,
            "time_mode": ctx.form.get("time_mode", PANEL_DEFAULT_TIME_MODE).strip() or PANEL_DEFAULT_TIME_MODE,
            "manual_time": self.normalize_manual_time(ctx.form.get("manual_time", "").strip(), time_zone),
            "ntp_server": ctx.form.get("ntp_server", "").strip(),
            "ntp_port": ctx.form.get("ntp_port", str(PANEL_DEFAULT_NTP_PORT)).strip() or str(PANEL_DEFAULT_NTP_PORT),
            "ntp_interval": ctx.form.get("ntp_interval", str(PANEL_DEFAULT_NTP_INTERVAL)).strip() or str(PANEL_DEFAULT_NTP_INTERVAL),
            "face_auth_enabled": ctx.form.get("face_auth_enabled", "1"),
        }

    def apply_bulk_panel_settings(self, ctx: RequestContext, start_response):
        selected_ids = [
            int(value)
            for value in ctx.form.get("selected_panel_ids", "").split(",")
            if value.strip().isdigit()
        ]
        available_panels = {int(panel["id"]): panel for panel in self.db.list_panels(enabled_only=True)}
        panels = [available_panels[panel_id] for panel_id in selected_ids if panel_id in available_panels]
        if not panels:
            return self.flash_redirect(start_response, "/panels", "Выбери хотя бы одну включённую панель.")
        bulk_values = self.bulk_panel_values(ctx)
        applied = 0
        issues = 0
        for panel in panels:
            values = {key: panel[key] for key in panel.keys()}
            values.update(bulk_values)
            self.db.save_panel(values, panel_pk=int(panel["id"]))
            try:
                messages = self.apply_panel_device_settings(int(panel["id"]))
            except Exception:  # noqa: BLE001
                issues += 1
                logger.exception("Failed to apply bulk panel settings for panel id=%s", panel["id"])
                continue
            if any("не " in message.lower() or "ошиб" in message.lower() for message in messages):
                issues += 1
            else:
                applied += 1
        logger.info(
            "Bulk panel settings applied: selected=%s applied=%s issues=%s",
            len(panels),
            applied,
            issues,
        )
        summary = f"Общие настройки сохранены для {len(panels)} панелей."
        if applied:
            summary += f" Успешно подтверждены: {applied}."
        if issues:
            summary += f" Требуют проверки: {issues}."
        return self.flash_redirect(start_response, "/panels", summary)

    def panel_client(self, panel: sqlite3.Row) -> HikvisionISAPIClient:
        return HikvisionISAPIClient(row_to_panel_config(panel))

    def panel_xml_request(self, panel: sqlite3.Row, method: str, endpoint: str, xml_body: str) -> dict[str, Any]:
        return self.panel_client(panel).request_raw(
            method,
            endpoint,
            body=xml_body.encode("utf-8"),
            headers={"Content-Type": "application/xml", "Accept": "application/xml"},
        )

    def panel_ntp_address_xml(self, ntp_server: str) -> str:
        try:
            ipaddress.ip_address(ntp_server)
            return f"<addressingFormatType>ipaddress</addressingFormatType><ipAddress>{html.escape(ntp_server)}</ipAddress>"
        except ValueError:
            return f"<addressingFormatType>hostname</addressingFormatType><hostName>{html.escape(ntp_server)}</hostName>"

    def get_panel_time_zone(self, panel: sqlite3.Row) -> str:
        response = self.panel_client(panel).request_raw(
            "GET",
            "/ISAPI/System/time/timeZone",
            headers={"Accept": "text/plain"},
        )
        return str(response.get("raw", "")).strip()

    def get_panel_time_xml(self, panel: sqlite3.Row) -> str:
        response = self.panel_client(panel).request_raw(
            "GET",
            "/ISAPI/System/time",
            headers={"Accept": "application/xml, text/xml"},
        )
        xml_body = str(response.get("raw", "")).strip()
        if not xml_body:
            raise HikvisionApiError("Time: invalid content")
        return xml_body

    def get_panel_time_type(self, panel: sqlite3.Row) -> dict[str, Any]:
        response = self.panel_client(panel).request_raw(
            "GET",
            "/ISAPI/System/time/timeType?format=json",
            headers={"Accept": "application/json"},
        )
        time_type = response.get("TimeType")
        if not isinstance(time_type, dict):
            raise HikvisionApiError("TimeType: invalid content")
        return time_type

    def get_panel_ntp_xml(self, panel: sqlite3.Row) -> str:
        response = self.panel_client(panel).request_raw(
            "GET",
            "/ISAPI/System/time/ntpServers/1",
            headers={"Accept": "application/xml, text/xml"},
        )
        xml_body = str(response.get("raw", "")).strip()
        if not xml_body:
            raise HikvisionApiError("NTPServer: invalid content")
        return xml_body

    def build_panel_time_xml(self, panel: sqlite3.Row) -> str:
        current_xml = self.get_panel_time_xml(panel)
        root = ET.fromstring(current_xml)
        time_mode = root.find(".//{*}timeMode")
        if time_mode is None:
            raise HikvisionApiError("Time.timeMode: invalid content")
        current_mode = str(panel["time_mode"] or PANEL_DEFAULT_TIME_MODE).strip() or PANEL_DEFAULT_TIME_MODE
        time_mode.text = current_mode
        time_zone = root.find(".//{*}timeZone")
        if time_zone is not None:
            time_zone.text = str(panel["time_zone"] or PANEL_DEFAULT_TIMEZONE).strip() or PANEL_DEFAULT_TIMEZONE
        if current_mode == "manual":
            manual_time = str(panel["manual_time"] or "").strip()
            if manual_time:
                local_time = root.find(".//{*}localTime")
                if local_time is None:
                    raise HikvisionApiError("Time.localTime: invalid content")
                local_time.text = manual_time
        return ET.tostring(root, encoding="unicode", xml_declaration=True)

    def build_panel_ntp_xml(self, panel: sqlite3.Row) -> str:
        current_xml = self.get_panel_ntp_xml(panel)
        root = ET.fromstring(current_xml)
        # Build a parent-map in O(n) instead of the previous O(n²) nested loop
        parent_map = {child: parent for parent in root.iter() for child in parent}
        ntp_server = str(panel["ntp_server"] or "").strip() or "0.0.0.0"
        for tag in ("ipAddress", "ipv6Address", "hostName"):
            for node in root.findall(f".//{{*}}{tag}"):
                parent = parent_map.get(node, root)
                parent.remove(node)
        addressing_format = root.find(".//{*}addressingFormatType")
        if addressing_format is None:
            raise HikvisionApiError("NTPServer.addressingFormatType: invalid content")
        # Preserve the XML namespace of the root element so the inserted node matches.
        ns = root.tag.split("}")[0].lstrip("{") if "}" in root.tag else ""
        def _ns_tag(local: str) -> str:
            return f"{{{ns}}}{local}" if ns else local
        try:
            ipaddress.ip_address(ntp_server)
            addressing_format.text = "ipaddress"
            address_node = ET.Element(_ns_tag("ipAddress"))
        except ValueError:
            addressing_format.text = "hostname"
            address_node = ET.Element(_ns_tag("hostName"))
        address_node.text = ntp_server
        insert_index = 0
        for idx, child in enumerate(list(root)):
            if child.tag.endswith("addressingFormatType"):
                insert_index = idx + 1
                break
        root.insert(insert_index, address_node)
        port_node = root.find(".//{*}portNo")
        if port_node is not None:
            port_node.text = str(int(panel["ntp_port"] or PANEL_DEFAULT_NTP_PORT))
        interval_node = root.find(".//{*}synchronizeInterval")
        if interval_node is not None:
            interval_node.text = str(int(panel["ntp_interval"] or PANEL_DEFAULT_NTP_INTERVAL))
        return ET.tostring(root, encoding="unicode", xml_declaration=True)

    def build_panel_time_type_payload(self, panel: sqlite3.Row) -> bytes:
        current = self.get_panel_time_type(panel)
        payload = {"TimeType": dict(current)}
        payload["TimeType"]["displayFormat"] = str(panel["time_display_format"] or PANEL_DEFAULT_TIME_FORMAT).strip() or PANEL_DEFAULT_TIME_FORMAT
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def panel_face_auth_state(self, panel: sqlite3.Row) -> int:
        return 1 if to_bool(panel["face_auth_enabled"]) else 2

    def get_panel_card_reader_cfg(self, panel: sqlite3.Row) -> dict[str, Any]:
        response = self.panel_client(panel).request_raw(
            "GET",
            "/ISAPI/AccessControl/CardReaderCfg/1?format=json&cardReaderID=1",
            headers={"Accept": "application/json"},
        )
        card_reader_cfg = response.get("CardReaderCfg")
        if not isinstance(card_reader_cfg, dict):
            raise HikvisionApiError("CardReaderCfg: invalid content")
        return card_reader_cfg

    def build_panel_card_reader_payload(self, panel: sqlite3.Row) -> bytes:
        current = self.get_panel_card_reader_cfg(panel)
        payload = dict(current)
        payload["cardReaderNo"] = 1
        payload["faceRecogizeEnable"] = self.panel_face_auth_state(panel)
        body = {"CardReaderCfg": payload}
        return json.dumps(body, ensure_ascii=False).encode("utf-8")

    def apply_panel_time_settings(self, panel: sqlite3.Row) -> list[str]:
        messages: list[str] = []
        try:
            self.panel_client(panel).request_raw(
                "PUT",
                "/ISAPI/System/time/timeZone",
                body=str(panel["time_zone"] or PANEL_DEFAULT_TIMEZONE).encode("utf-8"),
                headers={"Content-Type": "text/plain", "Accept": "text/plain"},
            )
        except HikvisionApiError as exc:
            return [f"Часовой пояс не обновлён: {self.human_hik_error(str(exc))}"]
        try:
            current_tz = self.get_panel_time_zone(panel)
        except HikvisionApiError:
            current_tz = ""
        if current_tz == str(panel["time_zone"] or "").strip():
            messages.append(f"Часовой пояс применён на панели: {current_tz}.")
        else:
            messages.append("Часовой пояс отправлен на панель, но подтверждение чтением не получено.")
        try:
            self.panel_client(panel).request_raw(
                "PUT",
                "/ISAPI/System/time/timeType?format=json",
                body=self.build_panel_time_type_payload(panel),
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            current_type = self.get_panel_time_type(panel)
            current_format = str(current_type.get("displayFormat") or "").strip()
            expected_format = str(panel["time_display_format"] or "").strip()
            if current_format == expected_format:
                messages.append(f"Формат времени применён на панели: {current_format}.")
            else:
                messages.append("Формат времени отправлен на панель, но подтверждение чтением не получено.")
        except HikvisionApiError as exc:
            messages.append(f"Формат времени не обновлён: {self.human_hik_error(str(exc))}")
        try:
            self.panel_client(panel).request_raw(
                "PUT",
                "/ISAPI/System/time/ntpServers/1",
                body=self.build_panel_ntp_xml(panel).encode("utf-8"),
                headers={"Content-Type": "application/xml", "Accept": "application/xml, text/xml"},
            )
            ntp_xml = self.get_panel_ntp_xml(panel)
            expected_server = str(panel["ntp_server"] or "").strip()
            if expected_server and expected_server in ntp_xml:
                messages.append(f"NTP сервер применён на панели: {expected_server}.")
            else:
                messages.append("NTP сервер отправлен на панель, но подтверждение чтением не получено.")
        except HikvisionApiError as exc:
            messages.append(f"NTP сервер не обновлён: {self.human_hik_error(str(exc))}")
        try:
            self.panel_client(panel).request_raw(
                "PUT",
                "/ISAPI/System/time",
                body=self.build_panel_time_xml(panel).encode("utf-8"),
                headers={"Content-Type": "application/xml", "Accept": "application/xml, text/xml"},
            )
            time_xml = self.get_panel_time_xml(panel)
            expected_mode = str(panel["time_mode"] or PANEL_DEFAULT_TIME_MODE).strip() or PANEL_DEFAULT_TIME_MODE
            if f"<timeMode>{expected_mode}</timeMode>" in time_xml:
                mode_label = "NTP" if expected_mode == "NTP" else "ручной"
                messages.append(f"Режим времени применён на панели: {mode_label}.")
            else:
                messages.append("Режим времени отправлен на панель, но подтверждение чтением не получено.")
            if expected_mode == "manual":
                expected_manual_time = str(panel["manual_time"] or "").strip()
                if expected_manual_time:
                    if f"<localTime>{expected_manual_time}</localTime>" in time_xml:
                        messages.append(f"Ручное время применено на панели: {expected_manual_time}.")
                    else:
                        messages.append("Ручное время отправлено на панель, но подтверждение чтением не получено.")
        except HikvisionApiError as exc:
            messages.append(f"Режим времени не обновлён: {self.human_hik_error(str(exc))}")
        return messages

    def apply_panel_face_auth(self, panel: sqlite3.Row) -> list[str]:
        try:
            self.panel_client(panel).request_raw(
                "PUT",
                "/ISAPI/AccessControl/CardReaderCfg/1?format=json&cardReaderID=1",
                body=self.build_panel_card_reader_payload(panel),
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
        except HikvisionApiError as exc:
            return [f"Аутентификация лица не обновлена: {self.human_hik_error(str(exc))}"]
        try:
            current = self.get_panel_card_reader_cfg(panel)
            current_state = int(current.get("faceRecogizeEnable", 0) or 0)
        except (HikvisionApiError, TypeError, ValueError):
            current_state = 0
        expected_state = self.panel_face_auth_state(panel)
        if current_state == expected_state:
            if to_bool(panel["face_auth_enabled"]):
                return ["Аутентификация лица включена на панели."]
            return ["Аутентификация лица выключена на панели."]
        return ["Настройка аутентификации лица отправлена, но подтверждение чтением не получено."]

    def apply_panel_device_settings(self, panel_pk: int) -> list[str]:
        panel = self.db.get_panel(panel_pk)
        if not panel:
            return ["Панель не найдена, параметры не применены на устройство."]
        messages: list[str] = []
        messages.extend(self.apply_panel_time_settings(panel))
        messages.extend(self.apply_panel_face_auth(panel))
        return messages

    def journal_filters(self, ctx: RequestContext) -> tuple[dict[str, Any], str]:
        quick = self._query_param(ctx, "quick", "")
        filters = {
            "employee": self._query_param(ctx, "employee", ""),
            "panel_id": self._int_query_param(ctx, "panel_id"),
            "result": self._query_param(ctx, "result", ""),
            "unlock_method": self._query_param(ctx, "unlock_method", ""),
            "date_from": self._query_param(ctx, "date_from", ""),
            "date_to": self._query_param(ctx, "date_to", ""),
            "show_unmatched": self._query_param(
                ctx,
                "show_unmatched",
                "1" if to_bool(self.config.get("journal", "show_unmatched_events", True)) else "0",
            ),
        }
        today = dt.date.today()
        quick_filters = {
            "today": {"date_from": today.isoformat(), "date_to": today.isoformat()},
            "week": {
                "date_from": (today - dt.timedelta(days=7)).isoformat(),
                "date_to": today.isoformat(),
            },
            "granted": {"result": "granted"},
            "unknown": {"show_unmatched": "1"},
            "denied": {"result": "denied"},
        }
        if quick in quick_filters:
            filters.update(quick_filters[quick])
        return filters, quick

    def journal(self, ctx: RequestContext, start_response):
        filters, quick = self.journal_filters(ctx)
        limit = int(self.config.get("journal", "events_per_page", 50))
        rows = self.db.list_journal(filters, limit=limit)
        stats = self.db.journal_stats()
        panels = self.db.list_panels()
        return self.render(
            start_response,
            "journal.html",
            {
                "title": "Журнал",
                "ctx": ctx,
                "rows": rows,
                "stats": stats,
                "filters": filters,
                "panels": panels,
                "quick": quick,
            },
        )

    def discrepancy_context(self) -> dict[str, Any]:
        panels = self.db.list_panels(enabled_only=True)
        health_by_id = self.panel_health_by_id()
        cache_by_id = self.panel_sync_cache_by_id()
        preview_by_panel_id: dict[int, dict[str, Any]] = {}
        for panel in panels:
            panel_id = int(panel["id"])
            cache_row = cache_by_id.get(panel_id)
            if not self.panel_sync_cache_is_fresh(cache_row):
                continue
            payload_json = str(cache_row["payload_json"] or "").strip()
            if not payload_json:
                continue
            try:
                preview_by_panel_id[panel_id] = json.loads(payload_json)
            except json.JSONDecodeError:
                continue
        employee_index: dict[str, dict[str, Any]] = {}
        panel_only: list[dict[str, Any]] = []

        for panel in panels:
            panel_id = int(panel["id"])
            item = preview_by_panel_id.get(panel_id)
            if not self.panel_is_available_for_ui(health_by_id.get(panel_id)):
                panel_only.append(
                    {
                        "type": "panel_error",
                        "panel": {"id": panel_id, "name": panel["name"]},
                        "message": self.panel_unavailable_title(health_by_id.get(panel_id)),
                    }
                )
                continue
            if not item:
                panel_only.append(
                    {
                        "type": "panel_error",
                        "panel": {"id": panel_id, "name": panel["name"]},
                        "message": "Нет свежих данных проверки",
                    }
                )
                continue
            panel_info = item["panel"]
            for row in item["creates"]:
                employee = row["employee"]
                current = employee_index.setdefault(employee["employee_id"], {"employee": employee, "issues": []})
                current["issues"].append({"panel": panel_info, "message": row["reasons"][0]})
            for row in item["updates"]:
                employee = row["employee"]
                current = employee_index.setdefault(employee["employee_id"], {"employee": employee, "issues": []})
                current["issues"].append({"panel": panel_info, "message": "; ".join(row["differences"])})
            for row in item["deletes"]:
                panel_only.append(
                    {
                        "type": "panel_only_user",
                        "panel": panel_info,
                        "employee_id": row.get("employeeNo"),
                        "name": row.get("name"),
                        "message": "; ".join(row.get("reasons", [])),
                    }
                )

        employees = self.db.list_employees(include_inactive=False)
        return {
            "employee_issues": list(employee_index.values()),
            "panel_only": panel_only,
            "employees": {employee["employee_id"]: employee for employee in employees},
        }

    def discrepancies(self, ctx: RequestContext, start_response):
        return self.render(
            start_response,
            "discrepancies.html",
            {
                "title": "Расхождения",
                "ctx": ctx,
                **self.discrepancy_context(),
            },
        )

    def sync_run_filters(self, ctx: RequestContext) -> dict[str, Any]:
        return {
            "run_id": self._int_query_param(ctx, "run_id"),
            "panel_id": self._int_query_param(ctx, "panel_id"),
            "employee_id": self._query_param(ctx, "employee_id", "").strip(),
            "status": self._query_param(ctx, "status", "").strip(),
        }

    def sync_runs(self, ctx: RequestContext, start_response):
        runs = self.db.latest_sync_runs(limit=30)
        filters = self.sync_run_filters(ctx)
        selected_run_id = filters["run_id"]
        if selected_run_id is None and runs:
            selected_run_id = int(runs[0]["id"])
        run = self.db.get_sync_run(selected_run_id) if selected_run_id else None
        actions = self.filtered_sync_actions(
            selected_run_id,
            panel_filter=filters["panel_id"],
            employee_filter=filters["employee_id"],
            status_filter=filters["status"],
        )
        return self.render(
            start_response,
            "sync_runs.html",
            {
                "title": "История синхронизаций",
                "ctx": ctx,
                "runs": runs,
                "selected_run_id": selected_run_id,
                "run": run,
                "actions": actions,
                "panels": self.db.list_panels(),
                "filters": {
                    "panel_id": filters["panel_id"],
                    "employee_id": filters["employee_id"],
                    "status": filters["status"],
                },
            },
        )

    def journal_detail(self, path: str, ctx: RequestContext, start_response):
        parts = path.strip("/").split("/")
        if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
            start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
            return [b"Not Found"]
        event_id = int(parts[1])
        row = self.db.get_event(event_id)
        return self.render(
            start_response,
            "journal_detail.html",
            {"title": f"Событие {event_id}", "ctx": ctx, "event": row},
        )

    @staticmethod
    def _form_int(form: dict[str, Any], key: str, default: int) -> int:
        try:
            return int(form.get(key, default) or default)
        except (ValueError, TypeError):
            return default

    def settings_payload(self, ctx: RequestContext) -> dict[str, dict[str, Any]]:
        f = ctx.form
        return {
            "retention": {
                "event_retention_days": self._form_int(f, "event_retention_days", 365),
                "snapshot_retention_days": self._form_int(f, "snapshot_retention_days", 60),
                "delete_events_enabled": to_bool(f.get("delete_events_enabled")),
                "delete_snapshots_enabled": to_bool(f.get("delete_snapshots_enabled")),
                "cleanup_interval_minutes": self._form_int(f, "cleanup_interval_minutes", 1440),
            },
            "journal": {
                "save_snapshots_for_granted": to_bool(f.get("save_snapshots_for_granted")),
                "save_snapshots_for_denied": to_bool(f.get("save_snapshots_for_denied")),
                "events_per_page": self._form_int(f, "events_per_page", 50),
                "show_unmatched_events": to_bool(f.get("show_unmatched_events")),
            },
            "server": {
                "timezone": f.get("timezone", self.config.get("server", "timezone", "Europe/Simferopol")),
                "panel_health_interval_minutes": self._form_int(
                    f, "panel_health_interval_minutes",
                    int(self.config.get("server", "panel_health_interval_minutes", 5) or 5),
                ),
            },
        }

    def settings(self, ctx: RequestContext, start_response):
        if ctx.environ.get("REQUEST_METHOD") == "POST":
            for section, values in self.settings_payload(ctx).items():
                self.config.update_section(section, values)
            self.config.reload()
            return self.redirect(start_response, "/settings")
        db_stats = self.db.journal_stats()
        media_stats = self.media_stats()
        config_view = {
            "retention": self.config.get_section("retention"),
            "journal": self.config.get_section("journal"),
            "server": self.config.get_section("server"),
        }
        return self.render(
            start_response,
            "settings.html",
            {"title": "Настройки", "ctx": ctx, "config": config_view, "db_stats": db_stats, "media_stats": media_stats},
        )

    def cleanup_run(self, ctx: RequestContext, start_response):
        result = self.db.cleanup_events(
            int(self.config.get("retention", "event_retention_days", 365))
            if to_bool(self.config.get("retention", "delete_events_enabled", True))
            else 0,
            int(self.config.get("retention", "snapshot_retention_days", 60))
            if to_bool(self.config.get("retention", "delete_snapshots_enabled", True))
            else 0,
        )
        event_media_dir = self.config.resolve_path("storage", "event_media_dir", "media/events")
        for relative in set(filter(None, result["snapshot_paths"])):
            target = event_media_dir / relative
            if target.exists():
                target.unlink(missing_ok=True)
        return self.redirect(start_response, "/settings")

    def media_stats(self) -> dict[str, Any]:
        root = self.config.resolve_path("storage", "event_media_dir", "media/events")
        total_size = 0
        files_count = 0
        if root.exists():
            for path in root.rglob("*"):
                if path.is_file():
                    files_count += 1
                    total_size += path.stat().st_size
        return {"files_count": files_count, "total_size_mb": round(total_size / (1024 * 1024), 2)}

    def sync_page(self, ctx: RequestContext, start_response):
        selected_panel_ids = []
        selected_employee_ids = []
        panel_id = self._int_query_param(ctx, "panel_id")
        employee_id = self._query_param(ctx, "employee_id", "").strip()
        employee_ids_query = self._query_param(ctx, "employee_ids", "").strip()
        if panel_id:
            selected_panel_ids = [panel_id]
        if employee_id:
            selected_employee_ids = [employee_id]
        elif employee_ids_query:
            selected_employee_ids = [value for value in employee_ids_query.split(",") if value.strip()]
        return self.render(
            start_response,
            "sync.html",
            self.sync_page_context(ctx, panel_ids=selected_panel_ids, employee_ids=selected_employee_ids),
        )

    def sync_preview(self, ctx: RequestContext, start_response):
        panel_ids, employee_ids = self.parse_sync_selection(ctx)
        preview = self.sync_service.sync_preview(panel_ids=panel_ids or None, employee_ids=employee_ids or None)
        if not employee_ids:
            self.store_sync_cache(preview)
        return self.render(
            start_response,
            "sync.html",
            self.sync_page_context(ctx, panel_ids=panel_ids, employee_ids=employee_ids, preview=preview),
        )

    def sync_apply(self, ctx: RequestContext, start_response):
        panel_ids, employee_ids = self.parse_sync_selection(ctx)
        result = self.sync_service.apply_sync(
            allow_delete=True,
            panel_ids=panel_ids or None,
            employee_ids=employee_ids or None,
        )
        try:
            self.refresh_sync_cache(panel_ids=panel_ids or None)
        except Exception:  # noqa: BLE001
            pass
        run_actions = self.db.list_sync_actions(result["run_id"])
        return self.render(
            start_response,
            "sync.html",
            self.sync_page_context(
                ctx,
                panel_ids=panel_ids,
                employee_ids=employee_ids,
                sync_result=result,
                run_actions=run_actions,
            ),
        )

    def employee_status_from_panel_preview(
        self,
        employee: sqlite3.Row,
        panel_preview: dict[str, Any] | None,
    ) -> dict[str, str]:
        if panel_preview is None:
            return {"icon": "help_outline", "color": "muted", "title": "Нет данных"}
        if panel_preview["error"]:
            return {
                "icon": "error_outline",
                "color": "danger",
                "title": panel_preview["error"],
            }

        employee_id = str(employee["employee_id"])
        current_state = employee_state(employee)
        if current_state != EMPLOYEE_STATE_ACTIVE:
            is_still_on_panel = any(
                str(panel_user.get("employeeNo")) == employee_id
                for panel_user in panel_preview.get("deletes", [])
            )
            if is_still_on_panel:
                return {
                    "icon": "remove_circle_outline",
                    "color": "danger",
                    "title": "Деактивирован локально, но всё ещё найден на панели",
                }
            return {
                "icon": "pause_circle_outline",
                "color": "muted",
                "title": "Сотрудник деактивирован локально",
            }
        if any(item["employee"]["employee_id"] == employee_id for item in panel_preview["creates"]):
            return {
                "icon": "cloud_off",
                "color": "warn",
                "title": "Есть в базе, но отсутствует на панели",
            }
        if any(item["employee"]["employee_id"] == employee_id for item in panel_preview["updates"]):
            return {
                "icon": "sync_problem",
                "color": "warn",
                "title": "Данные сотрудника на панели отличаются",
            }
        return {
            "icon": "check_circle",
            "color": "ok",
            "title": "Сотрудник синхронизирован",
        }

    def panel_health_by_id(self) -> dict[int, sqlite3.Row]:
        return {int(row["panel_id"]): row for row in self.db.list_panel_health_status()}

    def panel_is_available_for_ui(self, status_row: sqlite3.Row | None) -> bool:
        if not status_row or str(status_row["state"]) != "healthy":
            return False
        interval_minutes = int(self.config.get("server", "panel_health_interval_minutes", 5) or 5)
        max_age = dt.timedelta(minutes=max(2, interval_minutes * 2))
        return is_recent_iso(str(status_row["updated_at"] or ""), max_age)

    def panel_unavailable_title(self, status_row: sqlite3.Row | None) -> str:
        if not status_row:
            return "Проверка доступности ещё не выполнена"
        if str(status_row["state"]) == "healthy":
            return "Нет свежей проверки доступности панели"
        message = self.human_hik_error(str(status_row["message"] or "").strip())
        return message or "Панель недоступна"

    def build_employee_sync_status(
        self,
        employees: list[sqlite3.Row],
        panels: list[sqlite3.Row],
    ) -> dict[int, dict[int, dict[str, str]]]:
        if not employees or not panels:
            return {}
        health_by_id = self.panel_health_by_id()
        cache_by_id = self.panel_sync_cache_by_id()
        preview_by_panel: dict[int, dict[str, Any]] = {}
        for panel in panels:
            panel_id = int(panel["id"])
            cache_row = cache_by_id.get(panel_id)
            if not self.panel_sync_cache_is_fresh(cache_row):
                continue
            payload_json = str(cache_row["payload_json"] or "").strip()
            if not payload_json:
                continue
            try:
                preview_by_panel[panel_id] = json.loads(payload_json)
            except json.JSONDecodeError:
                continue
        status_map: dict[int, dict[int, dict[str, str]]] = {}
        for employee in employees:
            employee_status: dict[int, dict[str, str]] = {}
            for panel in panels:
                panel_id = int(panel["id"])
                panel_status_row = health_by_id.get(panel_id)
                if not self.panel_is_available_for_ui(panel_status_row):
                    employee_status[panel_id] = {
                        "icon": "portable_wifi_off",
                        "color": "muted",
                        "title": self.panel_unavailable_title(panel_status_row),
                    }
                    continue
                panel_preview = preview_by_panel.get(panel_id)
                if panel_preview is None:
                    employee_status[panel_id] = {
                        "icon": "help_outline",
                        "color": "muted",
                        "title": "Нет свежего кэша синхронизации",
                    }
                    continue
                employee_status[panel_id] = self.employee_status_from_panel_preview(employee, panel_preview)
            status_map[employee["id"]] = employee_status
        return status_map

    def panel_sync_totals(self, item: dict[str, Any]) -> dict[str, int]:
        return {
            "creates": len(item.get("creates", [])),
            "updates": len(item.get("updates", [])),
            "deletes": len(item.get("deletes", [])),
        }

    def human_sync_action(self, value: str | None) -> str:
        mapping = {
            "create": "Создан",
            "update": "Обновлён",
            "delete": "Удалён",
            "preview": "Проверка",
        }
        return mapping.get(value or "", value or "—")

    def human_sync_status(self, value: str | None, message: str | None = None) -> str:
        if value == "ok" and message and ERR_DEVICE_USER_ALREADY_EXIST_FACE in message:
            return "Лицо уже загружено"
        mapping = {
            "ok": "Успешно",
            "error": "Ошибка",
            "running": "В процессе",
            "finished": "Завершено",
        }
        return mapping.get(value or "", value or "—")

    def human_hik_error(self, message: str | None) -> str:
        text = message or ""
        mapping = {
            ERR_DEVICE_USER_ALREADY_EXIST_FACE: "Лицо уже загружено на панель",
            ERR_NOT_SUPPORT: "Функция не поддерживается этой прошивкой",
            ERR_METHOD_NOT_ALLOWED: "Метод не поддерживается устройством",
            "pictureModelingFailed": "Фото не подошло для построения биометрии",
            "MessageParametersLack": "Для запроса не хватает обязательных полей",
            "badXmlContent": "Устройство не приняло формат запроса",
            "invalidID": "Прошивка не поддерживает этот идентификатор или канал",
        }
        for key, label in mapping.items():
            if key in text:
                return label
        return text or "—"

    def human_result(self, value: str | None) -> str:
        mapping = {
            "granted": "Успешно",
            "denied": "Отказ",
            "unknown": "Не определено",
        }
        return mapping.get(value or "", value or "—")

    def human_method(self, value: str | None) -> str:
        mapping = {
            "card": "Карта",
            "face": "Лицо",
            "remote": "Удаленно",
            "qrCode": "QR",
            "password": "Пароль",
            "unknown": "Не определено",
        }
        return mapping.get(value or "", value or "—")

    def _query_param(self, ctx: RequestContext, key: str, default: str = "") -> str:
        query = parse_qs(ctx.environ.get("QUERY_STRING", ""), keep_blank_values=True)
        return query.get(key, [default])[-1]

    def _int_query_param(self, ctx: RequestContext, key: str) -> int | None:
        value = self._query_param(ctx, key, "")
        return int(value) if value.isdigit() else None

    def serve_media(self, path: str, start_response):
        relative = unquote(path[len("/media/") :])
        normalized = posixpath.normpath("/" + relative).lstrip("/")
        media_roots = [
            self.config.resolve_path("storage", "employee_media_dir", "media/employees"),
            self.config.resolve_path("storage", "event_media_dir", "media/events"),
        ]
        for root in media_roots:
            target = root / normalized
            try:
                target.resolve().relative_to(root.resolve())
            except ValueError:
                continue
            if not (target.exists() and target.is_file()):
                continue
            suffix = target.suffix.lower()
            if suffix == ".png":
                content_type = "image/png"
            elif suffix in (".jpg", ".jpeg"):
                content_type = "image/jpeg"
            else:
                continue
            start_response("200 OK", [("Content-Type", content_type)])
            return [target.read_bytes()]
        start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"Not Found"]

    def serve_static(self, path: str, start_response):
        relative = path[len("/static/") :]
        normalized = posixpath.normpath("/" + relative).lstrip("/")
        root = Path(__file__).parent / "static"
        target = root / normalized
        try:
            target.resolve().relative_to(root.resolve())
        except ValueError:
            start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
            return [b"Not Found"]
        if not (target.exists() and target.is_file()):
            start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
            return [b"Not Found"]
        suffix = target.suffix.lower()
        content_type = {
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
        }.get(suffix)
        if not content_type:
            start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
            return [b"Not Found"]
        start_response("200 OK", [("Content-Type", content_type)])
        return [target.read_bytes()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=APP_NAME)
    parser.add_argument("--config", default="config.yaml", help="Path to YAML config")
    parser.add_argument("--hash-password", help="Generate password hash and exit")
    return parser.parse_args()


def create_runtime(config_path: str | Path = "config.yaml", *, setup_logs: bool = True) -> AppRuntime:
    config = ConfigManager(Path(config_path))
    if setup_logs:
        log_file = setup_logging(config)
        logger.info("Logging initialized: %s", log_file)
    if config.secret_key in {"change-me", "change-this-secret-key", ""}:
        logger.warning("secret_key uses a default/insecure value. Set a strong random key in config.yaml.")

    db_path = config.resolve_path("storage", "db_path", "app.db")
    db = Database(db_path)
    bootstrap_path = config.resolve_path("storage", "bootstrap_panels_json", "panels.json")
    db.seed_panels_from_json(bootstrap_path)

    media_defaults = {"employee_media_dir": "media/employees", "event_media_dir": "media/events"}
    for folder_key, folder_default in media_defaults.items():
        ensure_dir(config.resolve_path("storage", folder_key, folder_default))

    app = AdminApp(config, db)
    supervisor = EventSupervisor(db, config)
    supervisor.start()
    cleanup = CleanupWorker(db, config)
    cleanup.start()
    runtime = AppRuntime(config=config, db=db, app=app, supervisor=supervisor, cleanup=cleanup)
    setattr(app, "runtime", runtime)
    atexit.register(runtime.stop)
    logger.info("Application runtime started")
    return runtime


def create_app(config_path: str | Path = "config.yaml") -> AdminApp:
    return create_runtime(config_path, setup_logs=True).app


def serve_app(app: AdminApp, host: str, port: int, config: ConfigManager) -> int:
    threads = int(config.get("server", "threads", 8) or 8)
    try:
        from waitress import serve

        logger.info("Serving with waitress on http://%s:%s (threads=%s)", host, port, threads)
        serve(app, host=host, port=port, threads=threads)
        return 0
    except ImportError:
        logger.warning("waitress is not installed, falling back to wsgiref. Install requirements.txt for production use.")
        from wsgiref.simple_server import make_server

        with make_server(host, port, app) as server:
            logger.info("Fallback server started on http://%s:%s", host, port)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
        return 0


def main() -> int:
    args = parse_args()
    if args.hash_password:
        print(generate_password_hash(args.hash_password))
        return 0

    runtime = create_runtime(Path(args.config), setup_logs=True)
    host = str(runtime.config.get("server", "host", "127.0.0.1"))
    port = int(runtime.config.get("server", "port", 8080))
    try:
        return serve_app(runtime.app, host, port, runtime.config)
    finally:
        runtime.stop()


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from admin_common import (
    ACCESS_EVENT_TYPE,
    EMPLOYEE_STATE_DEACTIVATED,
    EMPLOYEE_STATE_DELETED,
    ConfigManager,
    employee_is_active,
    employee_state,
    ensure_dir,
    now_local_iso,
    to_bool,
)
from admin_db import Database, row_to_panel_config
from hikvision_multi_panel import (
    ERR_CARD_ALREADY_EXIST,
    ERR_DEVICE_USER_ALREADY_EXIST_FACE,
    ERR_METHOD_NOT_ALLOWED,
    ERR_NOT_SUPPORT,
    HikvisionApiError,
    HikvisionEventStreamClient,
    HikvisionISAPIClient,
    MultipartStreamParser,
)

logger = logging.getLogger(__name__)


class HikvisionSyncService:
    def __init__(self, db: Database, employee_media_dir: Path):
        self.db = db
        self.employee_media_dir = employee_media_dir

    def default_valid_payload(self) -> dict[str, Any]:
        begin = dt.datetime.now().astimezone().replace(microsecond=0)
        end = begin.replace(year=2037, month=12, day=31, hour=23, minute=59, second=59)
        return {
            "enable": True,
            "timeType": "local",
            "beginTime": begin.isoformat(timespec="seconds"),
            "endTime": end.isoformat(timespec="seconds"),
        }

    def user_payload(self, employee: sqlite3.Row) -> dict[str, Any]:
        return {
            "employeeNo": employee["employee_id"],
            "name": f"{employee['first_name']} {employee['last_name']}".strip(),
            "userType": "normal",
            "closeDelayEnabled": False,
            "roomNo": employee["room_number"] or "",
            "Valid": self.default_valid_payload(),
        }

    def list_panel_users(self, panel: sqlite3.Row) -> tuple[list[dict[str, Any]], str | None]:
        client = HikvisionISAPIClient(row_to_panel_config(panel))
        page_size = 50
        start_positions = [0, 1]
        all_users: list[dict[str, Any]] = []
        for start_pos in start_positions:
            try:
                response = client.request_json(
                    "POST",
                    "/ISAPI/AccessControl/UserInfo/Search?format=json",
                    {"UserInfoSearchCond": {
                        "searchID": "1",
                        "searchResultPosition": start_pos,
                        "maxResults": page_size,
                    }},
                )
                page = self._extract_users(response)
                all_users.extend(page)
                if len(page) < page_size:
                    return all_users, None
                position = start_pos + page_size
                while True:
                    response = client.request_json(
                        "POST",
                        "/ISAPI/AccessControl/UserInfo/Search?format=json",
                        {"UserInfoSearchCond": {
                            "searchID": "1",
                            "searchResultPosition": position,
                            "maxResults": page_size,
                        }},
                    )
                    page = self._extract_users(response)
                    all_users.extend(page)
                    if len(page) < page_size:
                        break
                    position += page_size
                return all_users, None
            except Exception as exc:  # noqa: BLE001
                if start_pos == start_positions[-1]:
                    return [], str(exc)
        return [], None

    def _extract_users(self, response: dict[str, Any]) -> list[dict[str, Any]]:
        candidates = [
            response.get("UserInfoSearch"),
            response.get("UserInfoSearchCond"),
            response,
        ]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            users = candidate.get("UserInfo")
            if isinstance(users, list):
                return users
            if isinstance(users, dict):
                return [users]
        return []

    def delete_panel_user(
        self,
        panel: sqlite3.Row,
        employee_id: str,
        panel_user: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        client = HikvisionISAPIClient(row_to_panel_config(panel))
        payload = {"UserInfoDetail": {"mode": "byEmployeeNo", "EmployeeNoList": [{"employeeNo": employee_id}]}}
        try:
            return client.request_json("PUT", "/ISAPI/AccessControl/UserInfoDetail/Delete?format=json", payload)
        except HikvisionApiError as exc:
            if ERR_NOT_SUPPORT not in str(exc):
                raise
        fallback_payload = {
            "UserInfo": {
                "employeeNo": employee_id,
                "name": str((panel_user or {}).get("name") or employee_id),
                "userType": str((panel_user or {}).get("userType") or "normal"),
                "closeDelayEnabled": bool((panel_user or {}).get("closeDelayEnabled", False)),
                "roomNo": str((panel_user or {}).get("roomNo") or (panel_user or {}).get("roomNumber") or ""),
                "Valid": (panel_user or {}).get("Valid") or self.default_valid_payload(),
                "deleteUser": True,
            }
        }
        return client.request_json("PUT", "/ISAPI/AccessControl/UserInfo/SetUp?format=json", fallback_payload)

    def sync_preview(self, panel_ids: list[int] | None = None, employee_ids: list[str] | None = None) -> dict[str, Any]:
        selected_panels = [row for row in self.db.list_panels(enabled_only=True) if not panel_ids or row["id"] in panel_ids]
        all_employees = self.db.list_employees(include_inactive=True)
        active_employees = [row for row in all_employees if employee_is_active(row)]
        active_index = {row["employee_id"]: row for row in active_employees}
        all_employee_index = {row["employee_id"]: row for row in all_employees}
        employees = active_employees
        target_ids = set(employee_ids or [])
        if employee_ids:
            employees = [row for row in employees if row["employee_id"] in employee_ids]
        local_index = {row["employee_id"]: row for row in employees}
        panels_preview = []
        for panel in selected_panels:
            panel_users, error = self.list_panel_users(panel)
            panel_index = {str(user.get("employeeNo")): user for user in panel_users if user.get("employeeNo")}
            creates = []
            updates = []
            deletes = []
            if error:
                panels_preview.append({"panel": panel, "error": error, "creates": [], "updates": [], "deletes": []})
                continue
            for employee_id, employee in local_index.items():
                current = panel_index.get(employee_id)
                if current is None:
                    creates.append({"employee": employee, "reasons": ["Пользователь отсутствует на панели"]})
                    continue
                differences = self._employee_differences(employee, current)
                sync_reason = self._needs_resync(panel["id"], employee)
                if sync_reason:
                    differences.append(sync_reason)
                if differences:
                    updates.append({"employee": employee, "panel_user": current, "differences": differences})
            delete_scope_ids = target_ids if employee_ids else set(panel_index.keys())
            for employee_id, panel_user in panel_index.items():
                if employee_id not in delete_scope_ids:
                    continue
                if employee_id in active_index and (not employee_ids or employee_id in local_index):
                    continue
                local_employee = all_employee_index.get(employee_id)
                local_state = employee_state(local_employee) if local_employee else None
                if local_state == EMPLOYEE_STATE_DEACTIVATED:
                    reasons = ["Пользователь найден на панели, но в локальной БД деактивирован"]
                elif local_state == EMPLOYEE_STATE_DELETED:
                    reasons = ["Пользователь найден на панели, но в локальной БД помечен как удалённый"]
                else:
                    reasons = ["Пользователь найден на панели, но отсутствует в локальной БД"]
                deletes.append(
                    {
                        "panel_user": panel_user,
                        "employeeNo": panel_user.get("employeeNo"),
                        "name": panel_user.get("name"),
                        "reasons": reasons,
                        "local_employee": local_employee,
                    }
                )
            panels_preview.append(
                {
                    "panel": panel,
                    "error": None,
                    "creates": creates,
                    "updates": updates,
                    "deletes": deletes,
                }
            )
        return {"panels": panels_preview}

    def _employee_differs(self, employee: sqlite3.Row, panel_user: dict[str, Any]) -> bool:
        return bool(self._employee_differences(employee, panel_user))

    def _employee_differences(self, employee: sqlite3.Row, panel_user: dict[str, Any]) -> list[str]:
        full_name = f"{employee['first_name']} {employee['last_name']}".strip()
        room_number = employee["room_number"] or ""
        panel_name = str(panel_user.get("name", "")).strip()
        room_field_present = "roomNo" in panel_user or "roomNumber" in panel_user
        panel_room = str(panel_user.get("roomNo") or panel_user.get("roomNumber") or "").strip()
        differences: list[str] = []
        if panel_name != full_name:
            differences.append(f"Имя: '{panel_name or 'пусто'}' -> '{full_name}'")
        if room_field_present and panel_room != room_number:
            differences.append(f"Комната: '{panel_room or 'пусто'}' -> '{room_number or 'пусто'}'")
        return differences

    def _needs_resync(self, panel_id: int, employee: sqlite3.Row) -> str | None:
        last_sync = self.db.last_successful_sync_action(panel_id, employee["employee_id"])
        if not last_sync:
            return None
        if str(employee["updated_at"] or "") > str(last_sync["created_at"] or ""):
            return "Локальная карточка изменена после последней синхронизации"
        return None

    def apply_sync(
        self,
        allow_delete: bool,
        panel_ids: list[int] | None = None,
        employee_ids: list[str] | None = None,
        preview: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if preview is None:
            preview = self.sync_preview(panel_ids=panel_ids, employee_ids=employee_ids)
        run_id = self.db.create_sync_run()
        logger.info(
            "Starting sync run %s (allow_delete=%s, panels=%s, employees=%s)",
            run_id,
            allow_delete,
            panel_ids or "all",
            employee_ids or "all",
        )
        summary = {"created": 0, "updated": 0, "deleted": 0, "errors": 0}
        for item in preview["panels"]:
            panel = item["panel"]
            if item["error"]:
                summary["errors"] += 1
                self.db.add_sync_action(run_id, panel["id"], None, "preview", "error", item["error"])
                logger.warning("Sync preview error for panel %s: %s", panel["name"], item["error"])
                continue
            client = HikvisionISAPIClient(row_to_panel_config(panel))
            for create_item in item["creates"]:
                employee = create_item["employee"]
                try:
                    client.upsert_user(self.user_payload(employee))
                    self._upload_photo_if_needed(client, employee)
                    self._upload_card_if_needed(client, employee)
                    summary["created"] += 1
                    self.db.add_sync_action(run_id, panel["id"], employee["employee_id"], "create", "ok")
                except Exception as exc:  # noqa: BLE001
                    summary["errors"] += 1
                    self.db.add_sync_action(run_id, panel["id"], employee["employee_id"], "create", "error", str(exc))
                    logger.exception("Failed to create employee %s on panel %s", employee["employee_id"], panel["name"])
            for update in item["updates"]:
                employee = update["employee"]
                try:
                    client.upsert_user(self.user_payload(employee))
                    self._upload_photo_if_needed(client, employee)
                    self._upload_card_if_needed(client, employee)
                    summary["updated"] += 1
                    self.db.add_sync_action(run_id, panel["id"], employee["employee_id"], "update", "ok")
                except Exception as exc:  # noqa: BLE001
                    summary["errors"] += 1
                    self.db.add_sync_action(run_id, panel["id"], employee["employee_id"], "update", "error", str(exc))
                    logger.exception("Failed to update employee %s on panel %s", employee["employee_id"], panel["name"])
            if allow_delete:
                for panel_user in item["deletes"]:
                    employee_id = str(panel_user.get("employeeNo"))
                    try:
                        self.delete_panel_user(panel, employee_id, panel_user=panel_user)
                        summary["deleted"] += 1
                        self.db.add_sync_action(run_id, panel["id"], employee_id, "delete", "ok")
                    except Exception as exc:  # noqa: BLE001
                        summary["errors"] += 1
                        self.db.add_sync_action(run_id, panel["id"], employee_id, "delete", "error", str(exc))
                        logger.exception("Failed to delete employee %s from panel %s", employee_id, panel["name"])
        self.db.finish_sync_run(run_id, "finished", summary)
        logger.info("Sync run %s finished: %s", run_id, summary)
        return {"run_id": run_id, "summary": summary, "preview": preview}

    def _upload_photo_if_needed(self, client: HikvisionISAPIClient, employee: sqlite3.Row) -> None:
        if not employee["photo_path"]:
            return
        photo_path = self.employee_media_dir / employee["photo_path"]
        if not photo_path.exists():
            return
        try:
            client.upload_face(
                employee_no=employee["employee_id"],
                name=f"{employee['first_name']} {employee['last_name']}".strip(),
                photo_path=str(photo_path),
            )
        except HikvisionApiError as exc:
            if ERR_DEVICE_USER_ALREADY_EXIST_FACE in str(exc):
                return
            raise

    def _upload_card_if_needed(self, client: HikvisionISAPIClient, employee: sqlite3.Row) -> None:
        if not employee["card_number"]:
            return
        payload = {
            "CardInfo": {
                "employeeNo": employee["employee_id"],
                "cardNo": employee["card_number"],
                "cardType": "normalCard",
            }
        }
        try:
            client.request_json("PUT", "/ISAPI/AccessControl/CardInfo/Record?format=json", payload)
        except HikvisionApiError as exc:
            text = str(exc)
            if ERR_METHOD_NOT_ALLOWED in text or ERR_NOT_SUPPORT in text or ERR_CARD_ALREADY_EXIST in text:
                return
            raise


def employee_row_to_dict(employee: sqlite3.Row | dict[str, Any] | None) -> dict[str, Any] | None:
    if not employee:
        return None
    row = dict(employee)
    row["lifecycle_state"] = employee_state(employee)
    return row


def preview_item_to_cache_payload(item: dict[str, Any]) -> dict[str, Any]:
    panel = item["panel"]
    return {
        "panel": {
            "id": int(panel["id"]),
            "name": str(panel["name"]),
            "host": str(panel["host"]),
        },
        "error": item.get("error"),
        "creates": [
            {
                "employee": employee_row_to_dict(row["employee"]),
                "reasons": list(row.get("reasons", [])),
            }
            for row in item.get("creates", [])
        ],
        "updates": [
            {
                "employee": employee_row_to_dict(row["employee"]),
                "panel_user": dict(row.get("panel_user") or {}),
                "differences": list(row.get("differences", [])),
            }
            for row in item.get("updates", [])
        ],
        "deletes": [
            {
                "panel_user": dict(row.get("panel_user") or {}),
                "employeeNo": row.get("employeeNo"),
                "name": row.get("name"),
                "reasons": list(row.get("reasons", [])),
                "local_employee": employee_row_to_dict(row.get("local_employee")),
            }
            for row in item.get("deletes", [])
        ],
    }


def normalize_access_event(raw_event: dict[str, Any]) -> dict[str, Any] | None:
    if raw_event.get("eventType") != ACCESS_EVENT_TYPE:
        return None
    access = raw_event.get("AccessControllerEvent") or {}
    unlock_method = str(access.get("unlockType") or "").strip()
    employee_id = str(access.get("employeeNoString") or "").strip() or None
    card_number = str(access.get("cardNo") or "").strip() or None
    if unlock_method:
        result = "granted"
    elif employee_id or card_number:
        result = "denied"
    else:
        result = "unknown"
    if not unlock_method:
        unlock_method = "unknown"
    return {
        "event_time": raw_event.get("dateTime") or now_local_iso(),
        "employee_id": employee_id,
        "employee_name": None,
        "room_number": None,
        "card_number": card_number,
        "event_kind": "access",
        "result": result,
        "unlock_method": unlock_method,
    }


class PanelEventListener(threading.Thread):
    def __init__(self, db: Database, config: ConfigManager, panel_row: sqlite3.Row):
        super().__init__(daemon=True, name=f"listener-{panel_row['name']}")
        self.db = db
        self.config = config
        self.panel = panel_row
        self._stop_event = threading.Event()
        self.event_media_dir = Path(config.get("storage", "event_media_dir", "media/events"))

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.db.update_listener_status(self.panel["id"], "connecting", "Opening alertStream")
                client = HikvisionEventStreamClient(row_to_panel_config(self.panel))
                client.panel.timeout = 120
                with client.open_stream() as response:
                    self.db.update_listener_status(self.panel["id"], "connected", "Listening for events")
                    logger.info("Event listener connected: %s (%s)", self.panel["name"], self.panel["host"])
                    self._consume(response)
            except Exception as exc:  # noqa: BLE001
                self.db.update_listener_status(self.panel["id"], "error", str(exc))
                logger.warning("Event listener error for %s: %s", self.panel["name"], exc)
                time.sleep(5)

    def _consume(self, response) -> None:
        parser = MultipartStreamParser(response)
        pending_event_id: int | None = None
        pending_should_save_photo = False
        for headers, body in parser.parts():
            if self._stop_event.is_set():
                return
            content_type = headers.get("content-type", "")
            if "application/json" in content_type:
                try:
                    payload = json.loads(body.decode("utf-8", errors="replace"))
                except json.JSONDecodeError:
                    continue
                normalized = normalize_access_event(payload)
                if normalized is None:
                    pending_event_id = None
                    pending_should_save_photo = False
                    continue
                pending_event_id, created = self.db.save_access_event(self.panel["id"], normalized, payload)
                if not created:
                    pending_event_id = None
                    logger.debug("Duplicate event ignored for panel %s", self.panel["name"])
                save_granted = to_bool(self.config.get("journal", "save_snapshots_for_granted", True))
                save_denied = to_bool(self.config.get("journal", "save_snapshots_for_denied", True))
                pending_should_save_photo = (
                    (normalized["result"] == "granted" and save_granted)
                    or (normalized["result"] != "granted" and save_denied)
                )
            elif "image/jpeg" in content_type and pending_event_id and pending_should_save_photo:
                target = self._save_snapshot(pending_event_id, body)
                self.db.attach_event_snapshot(pending_event_id, str(target.relative_to(self.event_media_dir)))
                pending_event_id = None
                pending_should_save_photo = False

    def _save_snapshot(self, event_id: int, image_bytes: bytes) -> Path:
        stamp = dt.datetime.now(dt.UTC).strftime("%Y/%m/%d")
        target = self.event_media_dir / stamp / f"{self.panel['name']}-{event_id}.jpg"
        ensure_dir(target.parent)
        target.write_bytes(image_bytes)
        return target


class EventSupervisor(threading.Thread):
    def __init__(self, db: Database, config: ConfigManager):
        super().__init__(daemon=True, name="event-supervisor")
        self.db = db
        self.config = config
        self.listeners: dict[int, PanelEventListener] = {}
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()
        for listener in self.listeners.values():
            listener.stop()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                enabled = {row["id"]: row for row in self.db.list_panels(enabled_only=True)}
                for panel_id, row in enabled.items():
                    listener = self.listeners.get(panel_id)
                    if listener is None or not listener.is_alive():
                        listener = PanelEventListener(self.db, self.config, row)
                        self.listeners[panel_id] = listener
                        listener.start()
                        logger.info("Started listener for panel %s (%s)", row["name"], row["host"])
                for panel_id in list(self.listeners):
                    if panel_id not in enabled:
                        self.listeners[panel_id].stop()
                        logger.info("Stopped listener for disabled panel id=%s", panel_id)
                        del self.listeners[panel_id]
            except Exception:  # noqa: BLE001
                logger.exception("Event supervisor iteration failed")
            time.sleep(20)


class CleanupWorker(threading.Thread):
    def __init__(self, db: Database, config: ConfigManager):
        super().__init__(daemon=True, name="cleanup-worker")
        self.db = db
        self.config = config
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            interval_minutes = int(self.config.get("retention", "cleanup_interval_minutes", 1440))
            try:
                if to_bool(self.config.get("retention", "delete_snapshots_enabled", True)) or to_bool(
                    self.config.get("retention", "delete_events_enabled", True)
                ):
                    result = self.db.cleanup_events(
                        int(self.config.get("retention", "event_retention_days", 365))
                        if to_bool(self.config.get("retention", "delete_events_enabled", True))
                        else 0,
                        int(self.config.get("retention", "snapshot_retention_days", 60))
                        if to_bool(self.config.get("retention", "delete_snapshots_enabled", True))
                        else 0,
                    )
                    event_media_dir = Path(self.config.get("storage", "event_media_dir", "media/events"))
                    for relative in set(filter(None, result["snapshot_paths"])):
                        target = event_media_dir / relative
                        if target.exists():
                            target.unlink(missing_ok=True)
                    logger.info(
                        "Cleanup finished: deleted_events=%s deleted_snapshots=%s",
                        result["deleted_events"],
                        len(set(filter(None, result["snapshot_paths"]))),
                    )
            except Exception:  # noqa: BLE001
                logger.exception("Cleanup worker iteration failed")
            time.sleep(max(60, interval_minutes * 60))


class PanelSyncCacheWorker(threading.Thread):
    def __init__(
        self,
        db: Database,
        config: ConfigManager,
        sync_service: HikvisionSyncService | None = None,
    ):
        super().__init__(daemon=True, name="panel-sync-cache-worker")
        self.db = db
        self.config = config
        self.sync_service = sync_service or HikvisionSyncService(
            db,
            Path(config.get("storage", "employee_media_dir", "media/employees")),
        )
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def refresh_once(self) -> None:
        for panel in self.db.list_panels(enabled_only=True):
            panel_id = int(panel["id"])
            try:
                preview = self.sync_service.sync_preview(panel_ids=[panel_id])
                item = preview["panels"][0] if preview["panels"] else {
                    "panel": panel,
                    "error": "Нет данных предпросмотра",
                    "creates": [],
                    "updates": [],
                    "deletes": [],
                }
                if item.get("error"):
                    self.db.update_panel_health_status(panel_id, "error", str(item.get("error") or ""))
                else:
                    users_count = (
                        len(item.get("updates", []))
                        + len(item.get("creates", []))
                        + len(item.get("deletes", []))
                    )
                    self.db.update_panel_health_status(
                        panel_id,
                        "healthy",
                        f"Панель доступна, diff обновлён ({users_count} изменений)",
                    )
                self.db.update_panel_sync_cache(
                    panel_id,
                    "error" if item.get("error") else "ok",
                    str(item.get("error") or ""),
                    preview_item_to_cache_payload(item),
                )
            except Exception as exc:  # noqa: BLE001
                self.db.update_panel_health_status(panel_id, "error", str(exc))
                self.db.update_panel_sync_cache(panel_id, "error", str(exc), None)
                logger.warning("Panel cache refresh error for %s: %s", panel["name"], exc)

    def run(self) -> None:
        while not self._stop_event.is_set():
            interval_minutes = int(
                self.config.get("server", "panel_sync_interval_minutes", self.config.get("server", "panel_health_interval_minutes", 5))
                or 5
            )
            self.refresh_once()
            time.sleep(max(60, interval_minutes * 60))

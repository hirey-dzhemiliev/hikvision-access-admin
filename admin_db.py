from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import sqlite3
import threading
from pathlib import Path
from typing import Any

from admin_common import (
    EMPLOYEE_STATE_ACTIVE,
    EMPLOYEE_STATE_DEACTIVATED,
    EMPLOYEE_STATE_DELETED,
    PANEL_DEFAULT_NTP_INTERVAL,
    PANEL_DEFAULT_NTP_PORT,
    PANEL_DEFAULT_TIMEZONE,
    PANEL_DEFAULT_TIME_FORMAT,
    PANEL_DEFAULT_TIME_MODE,
    employee_display_name,
    employee_is_active,
    employee_state,
    ensure_dir,
    normalize_employee_state,
    now_utc_iso,
    to_bool,
)
from hikvision_multi_panel import PanelConfig

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        ensure_dir(self.path.parent)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.configure_connection()
        self.init_schema()

    def configure_connection(self) -> None:
        with self.conn:
            self.conn.execute("PRAGMA journal_mode = WAL")
            self.conn.execute("PRAGMA synchronous = NORMAL")
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.conn.execute("PRAGMA busy_timeout = 5000")
            self.conn.execute("PRAGMA temp_store = MEMORY")
            self.conn.execute("PRAGMA wal_autocheckpoint = 1000")
        logger.info("SQLite configured: WAL enabled, busy_timeout=5000ms")

    def close(self) -> None:
        with self._lock:
            try:
                self.conn.close()
            except sqlite3.Error:
                logger.exception("Failed to close SQLite connection")

    def init_schema(self) -> None:
        with self.conn:
            self.conn.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS employees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    employee_id TEXT NOT NULL UNIQUE,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL,
                    room_number TEXT,
                    card_number TEXT UNIQUE,
                    photo_path TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    lifecycle_state TEXT NOT NULL DEFAULT 'active',
                    comment TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS panels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    host TEXT NOT NULL,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    time_zone TEXT NOT NULL DEFAULT 'CST-3:00:00',
                    time_display_format TEXT NOT NULL DEFAULT 'MM/dd/yyyy hh:mm',
                    time_mode TEXT NOT NULL DEFAULT 'manual',
                    manual_time TEXT,
                    ntp_server TEXT,
                    ntp_port INTEGER NOT NULL DEFAULT 123,
                    ntp_interval INTEGER NOT NULL DEFAULT 60,
                    face_auth_enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS access_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    panel_id INTEGER NOT NULL,
                    event_time TEXT NOT NULL,
                    employee_id TEXT,
                    employee_db_id INTEGER,
                    employee_name_snapshot TEXT,
                    room_number_snapshot TEXT,
                    card_number_snapshot TEXT,
                    event_kind TEXT NOT NULL,
                    result TEXT NOT NULL,
                    unlock_method TEXT NOT NULL,
                    snapshot_path TEXT,
                    raw_json TEXT NOT NULL,
                    event_hash TEXT NOT NULL UNIQUE,
                    is_matched INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(panel_id) REFERENCES panels(id) ON DELETE CASCADE,
                    FOREIGN KEY(employee_db_id) REFERENCES employees(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS listener_status (
                    panel_id INTEGER PRIMARY KEY,
                    state TEXT NOT NULL,
                    message TEXT,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(panel_id) REFERENCES panels(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS panel_health_status (
                    panel_id INTEGER PRIMARY KEY,
                    state TEXT NOT NULL,
                    message TEXT,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(panel_id) REFERENCES panels(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS panel_sync_cache (
                    panel_id INTEGER PRIMARY KEY,
                    state TEXT NOT NULL,
                    message TEXT,
                    payload_json TEXT,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(panel_id) REFERENCES panels(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS sync_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    summary_json TEXT
                );

                CREATE TABLE IF NOT EXISTS sync_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sync_run_id INTEGER NOT NULL,
                    panel_id INTEGER NOT NULL,
                    employee_id TEXT,
                    action TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(sync_run_id) REFERENCES sync_runs(id) ON DELETE CASCADE,
                    FOREIGN KEY(panel_id) REFERENCES panels(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_employees_employee_id ON employees(employee_id);
                CREATE INDEX IF NOT EXISTS idx_employees_card_number ON employees(card_number);
                CREATE INDEX IF NOT EXISTS idx_access_events_event_time ON access_events(event_time);
                CREATE INDEX IF NOT EXISTS idx_access_events_employee_id ON access_events(employee_id);
                CREATE INDEX IF NOT EXISTS idx_access_events_employee_db_id ON access_events(employee_db_id);
                CREATE INDEX IF NOT EXISTS idx_access_events_panel_id ON access_events(panel_id);
                CREATE INDEX IF NOT EXISTS idx_access_events_result ON access_events(result);
                CREATE INDEX IF NOT EXISTS idx_access_events_unlock_method ON access_events(unlock_method);
                CREATE INDEX IF NOT EXISTS idx_sync_actions_sync_run_id ON sync_actions(sync_run_id);
                """
            )
        self.ensure_employee_columns()
        self.ensure_panel_columns()

    def ensure_employee_columns(self) -> None:
        with self._lock, self.conn:
            columns = {
                row["name"]
                for row in self.conn.execute("PRAGMA table_info(employees)").fetchall()
            }
            if "lifecycle_state" not in columns:
                self.conn.execute(
                    f"ALTER TABLE employees ADD COLUMN lifecycle_state TEXT NOT NULL DEFAULT '{EMPLOYEE_STATE_ACTIVE}'"
                )
            self.conn.execute(
                """
                UPDATE employees
                SET lifecycle_state = CASE
                    WHEN COALESCE(lifecycle_state, '') = '' AND is_active = 1 THEN ?
                    WHEN COALESCE(lifecycle_state, '') = '' AND is_active = 0 THEN ?
                    ELSE lifecycle_state
                END
                """,
                (EMPLOYEE_STATE_ACTIVE, EMPLOYEE_STATE_DEACTIVATED),
            )
            self.conn.execute(
                """
                UPDATE employees
                SET room_number = '1'
                WHERE room_number IS NULL OR TRIM(room_number) = ''
                """
            )
            self.conn.execute(
                """
                UPDATE employees
                SET card_number = NULL
                WHERE card_number IS NOT NULL
                  AND LOWER(TRIM(card_number)) IN ('', 'none', 'null')
                """
            )

    def ensure_panel_columns(self) -> None:
        with self._lock, self.conn:
            columns = {
                row["name"]
                for row in self.conn.execute("PRAGMA table_info(panels)").fetchall()
            }
            additions = [
                ("time_zone", f"TEXT NOT NULL DEFAULT '{PANEL_DEFAULT_TIMEZONE}'"),
                ("time_display_format", f"TEXT NOT NULL DEFAULT '{PANEL_DEFAULT_TIME_FORMAT}'"),
                ("time_mode", f"TEXT NOT NULL DEFAULT '{PANEL_DEFAULT_TIME_MODE}'"),
                ("manual_time", "TEXT"),
                ("ntp_server", "TEXT"),
                ("ntp_port", f"INTEGER NOT NULL DEFAULT {PANEL_DEFAULT_NTP_PORT}"),
                ("ntp_interval", f"INTEGER NOT NULL DEFAULT {PANEL_DEFAULT_NTP_INTERVAL}"),
                ("face_auth_enabled", "INTEGER NOT NULL DEFAULT 1"),
            ]
            for name, ddl in additions:
                if name not in columns:
                    self.conn.execute(f"ALTER TABLE panels ADD COLUMN {name} {ddl}")

    def seed_panels_from_json(self, path: Path) -> None:
        if not path.exists():
            return
        if self.count_panels() > 0:
            return
        raw = json.loads(path.read_text(encoding="utf-8"))
        now = now_utc_iso()
        with self._lock, self.conn:
            for item in raw:
                self.conn.execute(
                    """
                    INSERT INTO panels(
                        name, host, username, password, enabled,
                        time_zone, time_display_format, time_mode, manual_time, ntp_server, ntp_port, ntp_interval, face_auth_enabled,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, 1, ?, ?, ?, NULL, NULL, ?, ?, 1, ?, ?)
                    """,
                    (
                        item["name"],
                        item["host"],
                        item["username"],
                        item["password"],
                        PANEL_DEFAULT_TIMEZONE,
                        PANEL_DEFAULT_TIME_FORMAT,
                        PANEL_DEFAULT_TIME_MODE,
                        PANEL_DEFAULT_NTP_PORT,
                        PANEL_DEFAULT_NTP_INTERVAL,
                        now,
                        now,
                    ),
                )

    def count_panels(self) -> int:
        with self._lock:
            return int(self.conn.execute("SELECT COUNT(*) FROM panels").fetchone()[0])

    def get_dashboard_stats(self) -> dict[str, int]:
        today = dt.date.today().isoformat()
        with self._lock:
            employees = int(
                self.conn.execute(
                    "SELECT COUNT(*) FROM employees WHERE lifecycle_state = ?",
                    (EMPLOYEE_STATE_ACTIVE,),
                ).fetchone()[0]
            )
            panels = int(self.conn.execute("SELECT COUNT(*) FROM panels WHERE enabled = 1").fetchone()[0])
            events_today = int(
                self.conn.execute(
                    "SELECT COUNT(*) FROM access_events WHERE date(event_time) = ?",
                    (today,),
                ).fetchone()[0]
            )
            denied = int(
                self.conn.execute("SELECT COUNT(*) FROM access_events WHERE result = 'denied'").fetchone()[0]
            )
        return {
            "employees": employees,
            "panels": panels,
            "events_today": events_today,
            "denied": denied,
        }

    def list_employees(
        self,
        include_inactive: bool = False,
        search: str = "",
        sort_by: str = "name",
        sort_dir: str = "asc",
    ) -> list[sqlite3.Row]:
        sql = "SELECT * FROM employees"
        params: list[Any] = []
        clauses = []
        if not include_inactive:
            clauses.append("lifecycle_state = ?")
            params.append(EMPLOYEE_STATE_ACTIVE)
        else:
            clauses.append("lifecycle_state != ?")
            params.append(EMPLOYEE_STATE_DELETED)
        if search:
            escaped = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            clauses.append(
                "(employee_id LIKE ? ESCAPE '\\' OR first_name LIKE ? ESCAPE '\\'"
                " OR last_name LIKE ? ESCAPE '\\' OR COALESCE(card_number,'') LIKE ? ESCAPE '\\'"
                " OR COALESCE(room_number,'') LIKE ? ESCAPE '\\')"
            )
            pattern = f"%{escaped}%"
            params.extend([pattern, pattern, pattern, pattern, pattern])
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        numeric_employee_id = (
            "TRIM(COALESCE(employee_id, '')) <> '' "
            "AND TRIM(COALESCE(employee_id, '')) NOT GLOB '*[^0-9]*'"
        )
        direction = "DESC" if str(sort_dir).lower() == "desc" else "ASC"
        order_sql = {
            "name": (
                "LOWER(TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))) "
                f"{direction}, employee_id ASC"
            ),
            "employee_id": (
                f"CASE WHEN {numeric_employee_id} THEN 0 ELSE 1 END ASC, "
                f"CASE WHEN {numeric_employee_id} THEN CAST(TRIM(employee_id) AS INTEGER) END {direction}, "
                f"LOWER(employee_id) {direction}, "
                "LOWER(TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))) ASC"
            ),
            "card_number": (
                "CASE WHEN COALESCE(TRIM(card_number), '') = '' THEN 1 ELSE 0 END ASC, "
                f"LOWER(COALESCE(card_number, '')) {direction}, "
                "LOWER(TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))) ASC"
            ),
            "status": (
                "CASE WHEN lifecycle_state = 'active' THEN 0 ELSE 1 END "
                f"{direction}, LOWER(TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))) ASC"
            ),
        }.get(str(sort_by), None)
        sql += " ORDER BY " + (order_sql or "LOWER(TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))) ASC, employee_id ASC")
        with self._lock:
            return list(self.conn.execute(sql, params).fetchall())

    def get_employee(self, employee_pk: int) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute("SELECT * FROM employees WHERE id = ?", (employee_pk,)).fetchone()

    def get_employee_by_employee_id(self, employee_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM employees WHERE employee_id = ? AND lifecycle_state != ?",
                (employee_id, EMPLOYEE_STATE_DELETED),
            ).fetchone()

    def get_employee_by_card(self, card_number: str) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM employees WHERE card_number = ? AND lifecycle_state != ?",
                (card_number, EMPLOYEE_STATE_DELETED),
            ).fetchone()

    def save_employee(self, values: dict[str, Any], employee_pk: int | None = None) -> int:
        now = now_utc_iso()
        with self._lock, self.conn:
            if employee_pk is None:
                cursor = self.conn.execute(
                    """
                    INSERT INTO employees(
                        employee_id, first_name, last_name, room_number, card_number,
                        photo_path, is_active, lifecycle_state, comment, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        values["employee_id"],
                        values["first_name"],
                        values["last_name"],
                        values.get("room_number"),
                        values.get("card_number") or None,
                        values.get("photo_path"),
                        1 if employee_is_active(values) else 0,
                        employee_state(values),
                        values.get("comment"),
                        now,
                        now,
                    ),
                )
                return int(cursor.lastrowid)

            self.conn.execute(
                """
                UPDATE employees
                SET employee_id = ?, first_name = ?, last_name = ?, room_number = ?,
                    card_number = ?, photo_path = ?, is_active = ?, lifecycle_state = ?, comment = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    values["employee_id"],
                    values["first_name"],
                    values["last_name"],
                    values.get("room_number"),
                    values.get("card_number") or None,
                    values.get("photo_path"),
                    1 if employee_is_active(values) else 0,
                    employee_state(values),
                    values.get("comment"),
                    now,
                    employee_pk,
                ),
            )
            return employee_pk

    def set_employee_active(self, employee_pk: int, is_active: bool) -> None:
        self.set_employee_state(
            employee_pk,
            EMPLOYEE_STATE_ACTIVE if is_active else EMPLOYEE_STATE_DEACTIVATED,
        )

    def set_employee_state(self, employee_pk: int, state: str) -> None:
        normalized_state = normalize_employee_state(state)
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE employees SET is_active = ?, lifecycle_state = ?, updated_at = ? WHERE id = ?",
                (
                    1 if normalized_state == EMPLOYEE_STATE_ACTIVE else 0,
                    normalized_state,
                    now_utc_iso(),
                    employee_pk,
                ),
            )

    def delete_employee(self, employee_pk: int) -> None:
        with self._lock, self.conn:
            self.conn.execute("DELETE FROM employees WHERE id = ?", (employee_pk,))

    def update_employee_photo(self, employee_pk: int, photo_path: str | None) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE employees SET photo_path = ?, updated_at = ? WHERE id = ?",
                (photo_path, now_utc_iso(), employee_pk),
            )

    def list_panels(self, enabled_only: bool = False) -> list[sqlite3.Row]:
        sql = "SELECT * FROM panels"
        if enabled_only:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY name"
        with self._lock:
            return list(self.conn.execute(sql).fetchall())

    def get_panel(self, panel_pk: int) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute("SELECT * FROM panels WHERE id = ?", (panel_pk,)).fetchone()

    def save_panel(self, values: dict[str, Any], panel_pk: int | None = None) -> int:
        now = now_utc_iso()
        with self._lock, self.conn:
            if panel_pk is None:
                cursor = self.conn.execute(
                    """
                    INSERT INTO panels(
                        name, host, username, password, enabled,
                        time_zone, time_display_format, time_mode, manual_time, ntp_server, ntp_port, ntp_interval, face_auth_enabled,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        values["name"],
                        values["host"],
                        values["username"],
                        values["password"],
                        1 if to_bool(values.get("enabled", True)) else 0,
                        values.get("time_zone") or PANEL_DEFAULT_TIMEZONE,
                        values.get("time_display_format") or PANEL_DEFAULT_TIME_FORMAT,
                        values.get("time_mode") or PANEL_DEFAULT_TIME_MODE,
                        values.get("manual_time") or None,
                        values.get("ntp_server") or None,
                        int(values.get("ntp_port") or PANEL_DEFAULT_NTP_PORT),
                        int(values.get("ntp_interval") or PANEL_DEFAULT_NTP_INTERVAL),
                        1 if to_bool(values.get("face_auth_enabled", True)) else 0,
                        now,
                        now,
                    ),
                )
                return int(cursor.lastrowid)
            self.conn.execute(
                """
                UPDATE panels
                SET name = ?, host = ?, username = ?, password = ?, enabled = ?,
                    time_zone = ?, time_display_format = ?, time_mode = ?, manual_time = ?, ntp_server = ?, ntp_port = ?, ntp_interval = ?,
                    face_auth_enabled = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    values["name"],
                    values["host"],
                    values["username"],
                    values["password"],
                    1 if to_bool(values.get("enabled", True)) else 0,
                    values.get("time_zone") or PANEL_DEFAULT_TIMEZONE,
                    values.get("time_display_format") or PANEL_DEFAULT_TIME_FORMAT,
                    values.get("time_mode") or PANEL_DEFAULT_TIME_MODE,
                    values.get("manual_time") or None,
                    values.get("ntp_server") or None,
                    int(values.get("ntp_port") or PANEL_DEFAULT_NTP_PORT),
                    int(values.get("ntp_interval") or PANEL_DEFAULT_NTP_INTERVAL),
                    1 if to_bool(values.get("face_auth_enabled", True)) else 0,
                    now,
                    panel_pk,
                ),
            )
            return panel_pk

    def delete_panel(self, panel_pk: int) -> None:
        with self._lock, self.conn:
            self.conn.execute("DELETE FROM panels WHERE id = ?", (panel_pk,))

    def match_employee(self, employee_id: str | None, card_number: str | None) -> sqlite3.Row | None:
        matched = None
        if employee_id:
            matched = self.get_employee_by_employee_id(employee_id)
        if matched is None and card_number:
            matched = self.get_employee_by_card(card_number)
        return matched

    def save_access_event(
        self,
        panel_id: int,
        normalized: dict[str, Any],
        raw_event: dict[str, Any],
    ) -> tuple[int | None, bool]:
        raw_json = json.dumps(raw_event, ensure_ascii=False, sort_keys=True)
        event_hash = hashlib.sha1(f"{panel_id}:{raw_json}".encode("utf-8")).hexdigest()
        employee = self.match_employee(normalized.get("employee_id"), normalized.get("card_number"))
        with self._lock, self.conn:
            existing = self.conn.execute(
                "SELECT id FROM access_events WHERE event_hash = ?",
                (event_hash,),
            ).fetchone()
            if existing:
                return int(existing["id"]), False
            cursor = self.conn.execute(
                """
                INSERT INTO access_events(
                    panel_id, event_time, employee_id, employee_db_id, employee_name_snapshot,
                    room_number_snapshot, card_number_snapshot, event_kind, result,
                    unlock_method, snapshot_path, raw_json, event_hash, is_matched, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    panel_id,
                    normalized["event_time"],
                    normalized.get("employee_id"),
                    employee["id"] if employee else None,
                    normalized.get("employee_name") or (employee_display_name(employee) if employee else None),
                    normalized.get("room_number") or (employee["room_number"] if employee else None),
                    normalized.get("card_number") or (employee["card_number"] if employee else None),
                    normalized["event_kind"],
                    normalized["result"],
                    normalized["unlock_method"],
                    None,
                    raw_json,
                    event_hash,
                    1 if employee else 0,
                    now_utc_iso(),
                ),
            )
            return int(cursor.lastrowid), True

    def attach_event_snapshot(self, event_id: int, relative_path: str) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE access_events SET snapshot_path = ? WHERE id = ?",
                (relative_path, event_id),
            )

    def update_listener_status(self, panel_id: int, state: str, message: str) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO listener_status(panel_id, state, message, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(panel_id) DO UPDATE SET
                    state = excluded.state,
                    message = excluded.message,
                    updated_at = excluded.updated_at
                """,
                (panel_id, state, message, now_utc_iso()),
            )

    def update_panel_health_status(self, panel_id: int, state: str, message: str) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO panel_health_status(panel_id, state, message, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(panel_id) DO UPDATE SET
                    state = excluded.state,
                    message = excluded.message,
                    updated_at = excluded.updated_at
                """,
                (panel_id, state, message, now_utc_iso()),
            )

    def list_panel_health_status(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    SELECT p.id AS panel_id, p.name, p.host, ph.state, ph.message, ph.updated_at
                    FROM panels p
                    LEFT JOIN panel_health_status ph ON ph.panel_id = p.id
                    ORDER BY p.name
                    """
                ).fetchall()
            )

    def update_panel_sync_cache(
        self,
        panel_id: int,
        state: str,
        message: str = "",
        payload: dict[str, Any] | None = None,
    ) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO panel_sync_cache(panel_id, state, message, payload_json, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(panel_id) DO UPDATE SET
                    state = excluded.state,
                    message = excluded.message,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (
                    panel_id,
                    state,
                    message,
                    json.dumps(payload, ensure_ascii=False) if payload is not None else None,
                    now_utc_iso(),
                ),
            )

    def get_panel_sync_cache(self, panel_id: int) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM panel_sync_cache WHERE panel_id = ?",
                (panel_id,),
            ).fetchone()

    def list_panel_sync_cache(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    SELECT psc.*, p.name, p.host
                    FROM panel_sync_cache psc
                    JOIN panels p ON p.id = psc.panel_id
                    ORDER BY p.name
                    """
                ).fetchall()
            )

    def list_journal(
        self,
        filters: dict[str, Any],
        limit: int = 100,
    ) -> list[sqlite3.Row]:
        sql = """
        SELECT ae.*, p.name AS panel_name
        FROM access_events ae
        JOIN panels p ON p.id = ae.panel_id
        """
        clauses = []
        params: list[Any] = []
        if filters.get("employee"):
            escaped = (
                filters["employee"]
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
            )
            pattern = f"%{escaped}%"
            clauses.append(
                "(COALESCE(ae.employee_name_snapshot,'') LIKE ? ESCAPE '\\'"
                " OR COALESCE(ae.employee_id,'') LIKE ? ESCAPE '\\'"
                " OR COALESCE(ae.card_number_snapshot,'') LIKE ? ESCAPE '\\')"
            )
            params.extend([pattern, pattern, pattern])
        if filters.get("panel_id"):
            clauses.append("ae.panel_id = ?")
            params.append(filters["panel_id"])
        if filters.get("result"):
            clauses.append("ae.result = ?")
            params.append(filters["result"])
        if filters.get("unlock_method"):
            clauses.append("ae.unlock_method = ?")
            params.append(filters["unlock_method"])
        if filters.get("date_from"):
            clauses.append("date(ae.event_time) >= date(?)")
            params.append(filters["date_from"])
        if filters.get("date_to"):
            clauses.append("date(ae.event_time) <= date(?)")
            params.append(filters["date_to"])
        if not to_bool(filters.get("show_unmatched", True)):
            clauses.append("ae.is_matched = 1")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY ae.created_at DESC, ae.id DESC LIMIT ?"
        params.append(limit)
        with self._lock:
            return list(self.conn.execute(sql, params).fetchall())

    def get_event(self, event_id: int) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                """
                SELECT ae.*, p.name AS panel_name
                FROM access_events ae
                JOIN panels p ON p.id = ae.panel_id
                WHERE ae.id = ?
                """,
                (event_id,),
            ).fetchone()

    def journal_stats(self) -> dict[str, Any]:
        with self._lock:
            total_events = int(self.conn.execute("SELECT COUNT(*) FROM access_events").fetchone()[0])
            unmatched_events = int(
                self.conn.execute("SELECT COUNT(*) FROM access_events WHERE is_matched = 0").fetchone()[0]
            )
            photos_count = int(
                self.conn.execute(
                    "SELECT COUNT(*) FROM access_events WHERE snapshot_path IS NOT NULL"
                ).fetchone()[0]
            )
            listener_rows = list(
                self.conn.execute(
                    """
                    SELECT p.id AS panel_id, p.name, p.host, ls.state, ls.message, ls.updated_at
                    FROM panels p
                    LEFT JOIN listener_status ls ON ls.panel_id = p.id
                    ORDER BY p.name
                    """
                ).fetchall()
            )
        return {
            "total_events": total_events,
            "unmatched_events": unmatched_events,
            "photos_count": photos_count,
            "listeners": listener_rows,
        }

    def latest_employee_events(self, employee_pk: int, limit: int = 20) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    SELECT ae.*, p.name AS panel_name
                    FROM access_events ae
                    JOIN panels p ON p.id = ae.panel_id
                    WHERE ae.employee_db_id = ?
                    ORDER BY ae.created_at DESC, ae.id DESC
                    LIMIT ?
                    """,
                    (employee_pk, limit),
                ).fetchall()
            )

    def create_sync_run(self) -> int:
        with self._lock, self.conn:
            cursor = self.conn.execute(
                "INSERT INTO sync_runs(started_at, status) VALUES (?, ?)",
                (now_utc_iso(), "running"),
            )
            return int(cursor.lastrowid)

    def finish_sync_run(self, sync_run_id: int, status: str, summary: dict[str, Any]) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE sync_runs SET finished_at = ?, status = ?, summary_json = ? WHERE id = ?",
                (now_utc_iso(), status, json.dumps(summary, ensure_ascii=False), sync_run_id),
            )

    def add_sync_action(
        self,
        sync_run_id: int,
        panel_id: int,
        employee_id: str | None,
        action: str,
        status: str,
        message: str = "",
    ) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO sync_actions(sync_run_id, panel_id, employee_id, action, status, message, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (sync_run_id, panel_id, employee_id, action, status, message, now_utc_iso()),
            )

    def latest_sync_runs(self, limit: int = 20) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    "SELECT * FROM sync_runs ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            )

    def last_successful_sync_action(self, panel_id: int, employee_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                """
                SELECT *
                FROM sync_actions
                WHERE panel_id = ?
                  AND employee_id = ?
                  AND status = 'ok'
                  AND action IN ('create', 'update')
                ORDER BY id DESC
                LIMIT 1
                """,
                (panel_id, employee_id),
            ).fetchone()

    def latest_sync_errors(self, limit: int = 10) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    SELECT sa.*, p.name AS panel_name
                    FROM sync_actions sa
                    JOIN panels p ON p.id = sa.panel_id
                    WHERE sa.status = 'error'
                    ORDER BY sa.id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
            )

    def employee_sync_actions(self, employee_id: str, limit: int = 10) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    SELECT sa.*, p.name AS panel_name
                    FROM sync_actions sa
                    JOIN panels p ON p.id = sa.panel_id
                    WHERE sa.employee_id = ?
                    ORDER BY sa.id DESC
                    LIMIT ?
                    """,
                    (employee_id, limit),
                ).fetchall()
            )

    def get_sync_run(self, sync_run_id: int) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM sync_runs WHERE id = ?",
                (sync_run_id,),
            ).fetchone()

    def list_sync_actions(self, sync_run_id: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    SELECT sa.*, p.name AS panel_name
                    FROM sync_actions sa
                    JOIN panels p ON p.id = sa.panel_id
                    WHERE sa.sync_run_id = ?
                    ORDER BY sa.id ASC
                    """,
                    (sync_run_id,),
                ).fetchall()
            )

    def cleanup_events(self, event_retention_days: int, snapshots_retention_days: int) -> dict[str, int]:
        deleted_events = 0
        deleted_snapshots = 0
        snapshot_paths_to_delete: list[str] = []
        with self._lock, self.conn:
            if snapshots_retention_days > 0:
                rows = list(
                    self.conn.execute(
                        """
                        SELECT id, snapshot_path
                        FROM access_events
                        WHERE snapshot_path IS NOT NULL
                          AND date(event_time) < date('now', ?)
                        """,
                        (f"-{snapshots_retention_days} day",),
                    ).fetchall()
                )
                for row in rows:
                    snapshot_paths_to_delete.append(row["snapshot_path"])
                self.conn.execute(
                    """
                    UPDATE access_events
                    SET snapshot_path = NULL
                    WHERE snapshot_path IS NOT NULL
                      AND date(event_time) < date('now', ?)
                    """,
                    (f"-{snapshots_retention_days} day",),
                )
                deleted_snapshots = len(snapshot_paths_to_delete)

            if event_retention_days > 0:
                rows = list(
                    self.conn.execute(
                        """
                        SELECT snapshot_path
                        FROM access_events
                        WHERE date(event_time) < date('now', ?)
                        """,
                        (f"-{event_retention_days} day",),
                    ).fetchall()
                )
                for row in rows:
                    if row["snapshot_path"]:
                        snapshot_paths_to_delete.append(row["snapshot_path"])
                cursor = self.conn.execute(
                    "DELETE FROM access_events WHERE date(event_time) < date('now', ?)",
                    (f"-{event_retention_days} day",),
                )
                deleted_events = cursor.rowcount
        return {
            "deleted_events": deleted_events,
            "deleted_snapshots": deleted_snapshots,
            "snapshot_paths": snapshot_paths_to_delete,
        }


def row_to_panel_config(row: sqlite3.Row) -> PanelConfig:
    return PanelConfig(
        name=row["name"],
        host=row["host"],
        username=row["username"],
        password=row["password"],
        protocol=str(dict(row).get("protocol", "http")),
    )

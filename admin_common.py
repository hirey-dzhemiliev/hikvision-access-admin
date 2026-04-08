from __future__ import annotations

import base64
import copy
import datetime as dt
import hashlib
import hmac
import logging
import os
import re
import threading
import time
import unicodedata
import xml.etree.ElementTree as ET
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import yaml


APP_NAME = "Hikvision Access Admin"
COOKIE_NAME = "hikvision_session"
SESSION_TTL_SECONDS = 60 * 60 * 8
ACCESS_EVENT_TYPE = "AccessControllerEvent"
MAX_REQUEST_BYTES = 10 * 1024 * 1024
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

PANEL_DEFAULT_TIMEZONE = "CST-3:00:00"
PANEL_DEFAULT_TIME_FORMAT = "MM/dd/yyyy hh:mm"
PANEL_DEFAULT_TIME_MODE = "manual"
PANEL_DEFAULT_NTP_PORT = 123
PANEL_DEFAULT_NTP_INTERVAL = 60

EMPLOYEE_STATE_ACTIVE = "active"
EMPLOYEE_STATE_DEACTIVATED = "deactivated"
EMPLOYEE_STATE_DELETED = "deleted"
EMPLOYEE_STATES = {
    EMPLOYEE_STATE_ACTIVE,
    EMPLOYEE_STATE_DEACTIVATED,
    EMPLOYEE_STATE_DELETED,
}


def source_value(source: Any, key: str, default: Any = None) -> Any:
    if isinstance(source, dict):
        return source.get(key, default)
    try:
        return source[key]  # type: ignore[index]
    except (TypeError, KeyError, IndexError):
        return default


def hikvision_timezone_from_utc_offset(hours: int) -> str:
    sign = "-" if hours >= 0 else "+"
    return f"CST{sign}{abs(hours)}:00:00"


def human_utc_offset_label(hours: int) -> str:
    sign = "+" if hours >= 0 else "-"
    return f"UTC{sign}{abs(hours):02d}:00"


PANEL_TIMEZONE_CHOICES = [
    {"value": hikvision_timezone_from_utc_offset(hours), "label": human_utc_offset_label(hours)}
    for hours in range(-12, 15)
]


def now_local_iso() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def now_utc_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


def parse_iso_datetime(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def is_recent_iso(value: str | None, max_age: dt.timedelta) -> bool:
    parsed = parse_iso_datetime(value)
    if parsed is None:
        return False
    return (dt.datetime.now(dt.UTC) - parsed) <= max_age


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def normalize_employee_state(value: Any, fallback_active: bool = True) -> str:
    raw = str(value or "").strip().lower()
    if raw in EMPLOYEE_STATES:
        return raw
    if raw in {"1", "true", "yes", "on"}:
        return EMPLOYEE_STATE_ACTIVE
    if raw in {"0", "false", "no", "off"}:
        return EMPLOYEE_STATE_DEACTIVATED
    return EMPLOYEE_STATE_ACTIVE if fallback_active else EMPLOYEE_STATE_DEACTIVATED


def employee_state(source: Any) -> str:
    if isinstance(source, dict) and "lifecycle_state" in source:
        return normalize_employee_state(source.get("lifecycle_state"))
    try:
        value = source["lifecycle_state"]  # type: ignore[index]
    except (TypeError, KeyError, IndexError):
        value = None
    if value is not None:
        return normalize_employee_state(value)
    try:
        is_active = source["is_active"]  # type: ignore[index]
    except (TypeError, KeyError, IndexError):
        is_active = None
    return EMPLOYEE_STATE_ACTIVE if to_bool(is_active if is_active is not None else True) else EMPLOYEE_STATE_DEACTIVATED


def employee_is_active(source: Any) -> bool:
    return employee_state(source) == EMPLOYEE_STATE_ACTIVE


def employee_display_name(source: Any) -> str:
    full_name = str(source_value(source, "full_name", "") or "").strip()
    if full_name:
        return full_name
    first_name = str(source_value(source, "first_name", "") or "").strip()
    last_name = str(source_value(source, "last_name", "") or "").strip()
    combined = " ".join(part for part in [first_name, last_name] if part).strip()
    if combined:
        return combined
    fallback = str(source_value(source, "employee_name_snapshot", "") or "").strip()
    if fallback:
        return fallback
    return str(source_value(source, "employee_id", "") or "").strip()


_CYRILLIC_TRANSLIT_MAP = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ё": "e",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "y",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "h",
    "ц": "ts",
    "ч": "ch",
    "ш": "sh",
    "щ": "sch",
    "ъ": "",
    "ы": "y",
    "ь": "",
    "э": "e",
    "ю": "yu",
    "я": "ya",
    "і": "i",
    "ї": "yi",
    "є": "e",
    "ґ": "g",
}


def panel_person_name(value: str | None, max_bytes: int = 32) -> str:
    raw = str(value or "").strip()
    transliterated_parts: list[str] = []
    for char in raw:
        lower = char.lower()
        if lower in _CYRILLIC_TRANSLIT_MAP:
            repl = _CYRILLIC_TRANSLIT_MAP[lower]
            if char.isupper() and repl:
                repl = repl[0].upper() + repl[1:]
            transliterated_parts.append(repl)
            continue
        transliterated_parts.append(char)
    transliterated = "".join(transliterated_parts)
    transliterated = unicodedata.normalize("NFKD", transliterated).encode("ascii", "ignore").decode("ascii")
    transliterated = re.sub(r"\s+", " ", transliterated).strip()
    result_bytes = bytearray()
    for char in transliterated:
        encoded = char.encode("utf-8")
        if len(result_bytes) + len(encoded) > max_bytes:
            break
        result_bytes.extend(encoded)
    return result_bytes.decode("utf-8", errors="ignore").strip()


def xml_text(node: ET.Element | None, name: str) -> str | None:
    if node is None:
        return None
    child = node.find(f".//{{*}}{name}")
    return child.text if child is not None else None


def generate_password_hash(password: str, iterations: int = 390000) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return (
        f"pbkdf2_sha256${iterations}$"
        f"{base64.b64encode(salt).decode('ascii')}$"
        f"{base64.b64encode(digest).decode('ascii')}"
    )


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, iterations, salt_b64, digest_b64 = password_hash.split("$", 3)
    except ValueError:
        return False
    if algorithm != "pbkdf2_sha256":
        return False
    salt = base64.b64decode(salt_b64)
    expected = base64.b64decode(digest_b64)
    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, int(iterations))
    return hmac.compare_digest(actual, expected)


class ConfigManager:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self.data = self.load()

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            raise FileNotFoundError(f"Config file not found: {self.path}")
        with self.path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        for section in ["auth", "server", "storage", "retention", "journal", "logging"]:
            data.setdefault(section, {})
        data["auth"].setdefault("users", [])
        return data

    def reload(self) -> None:
        with self._lock:
            self.data = self.load()

    def save(self) -> None:
        with self._lock:
            data_copy = copy.deepcopy(self.data)
        with self.path.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(data_copy, fh, sort_keys=False, allow_unicode=True)

    @property
    def auth_users(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self.data["auth"].get("users", []))

    @property
    def secret_key(self) -> str:
        with self._lock:
            return str(self.data["auth"].get("secret_key", "change-me"))

    @property
    def session_cookie_secure(self) -> bool:
        with self._lock:
            return to_bool(self.data.get("auth", {}).get("session_cookie_secure", False))

    def get(self, section: str, key: str, default: Any = None) -> Any:
        with self._lock:
            return self.data.get(section, {}).get(key, default)

    def get_section(self, section: str) -> dict[str, Any]:
        with self._lock:
            return dict(self.data.get(section, {}))

    def update_section(self, section: str, values: dict[str, Any]) -> None:
        with self._lock:
            self.data.setdefault(section, {}).update(values)
        self.save()

    def resolve_path(self, section: str, key: str, default: Any = None) -> Path:
        raw = self.get(section, key, default)
        path = Path(str(raw))
        if path.is_absolute():
            return path
        return (self.path.parent / path).resolve()


def setup_logging(config: ConfigManager) -> Path:
    log_file = config.resolve_path(
        "logging",
        "file",
        config.resolve_path("storage", "db_path", "app.db").parent / "logs" / "app.log",
    )
    ensure_dir(log_file.parent)
    level_name = str(config.get("logging", "level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    max_bytes = int(config.get("logging", "max_bytes", 5 * 1024 * 1024) or 5 * 1024 * 1024)
    backup_count = int(config.get("logging", "backup_count", 5) or 5)

    handlers: list[logging.Handler] = [
        RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"),
        logging.StreamHandler(),
    ]
    logging.basicConfig(level=level, format=LOG_FORMAT, handlers=handlers, force=True)
    logging.captureWarnings(True)
    return log_file


class SessionManager:
    def __init__(self, config: ConfigManager):
        self.config = config

    def create_cookie_value(self, username: str) -> str:
        expires = str(int(time.time()) + SESSION_TTL_SECONDS)
        payload = f"{username}|{expires}"
        signature = hmac.new(
            self.config.secret_key.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return f"{payload}|{signature}"

    def parse_cookie_value(self, cookie_value: str | None) -> str | None:
        if not cookie_value:
            return None
        try:
            username, expires, signature = cookie_value.split("|", 2)
        except ValueError:
            return None
        payload = f"{username}|{expires}"
        expected = hmac.new(
            self.config.secret_key.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(signature, expected):
            return None
        if int(expires) < int(time.time()):
            return None
        for user in self.config.auth_users:
            if user.get("username") == username and to_bool(user.get("is_active", True)):
                return username
        return None

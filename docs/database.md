# Схема базы данных

## Таблица `employees`

- `id INTEGER PRIMARY KEY`
- `employee_id TEXT NOT NULL UNIQUE`
- `first_name TEXT NOT NULL`
- `last_name TEXT NOT NULL`
- `room_number TEXT`
- `card_number TEXT UNIQUE`
- `photo_path TEXT`
- `is_active INTEGER NOT NULL DEFAULT 1`
- `lifecycle_state TEXT NOT NULL DEFAULT 'active'`
- `comment TEXT`
- `created_at TEXT NOT NULL`
- `updated_at TEXT NOT NULL`

## Таблица `panels`

- `id INTEGER PRIMARY KEY`
- `name TEXT NOT NULL UNIQUE`
- `host TEXT NOT NULL`
- `username TEXT NOT NULL`
- `password TEXT NOT NULL`
- `enabled INTEGER NOT NULL DEFAULT 1`
- `time_zone TEXT NOT NULL DEFAULT 'CST-3:00:00'`
- `time_display_format TEXT NOT NULL DEFAULT 'MM/dd/yyyy hh:mm'`
- `time_mode TEXT NOT NULL DEFAULT 'manual'`
- `manual_time TEXT`
- `ntp_server TEXT`
- `ntp_port INTEGER NOT NULL DEFAULT 123`
- `ntp_interval INTEGER NOT NULL DEFAULT 60`
- `face_auth_enabled INTEGER NOT NULL DEFAULT 1`
- `created_at TEXT NOT NULL`
- `updated_at TEXT NOT NULL`

## Таблица `panel_sync_cache`

- `panel_id INTEGER PRIMARY KEY`
- `state TEXT NOT NULL`
- `message TEXT`
- `payload_json TEXT`
- `updated_at TEXT NOT NULL`

Назначение: хранить diff синхронизации по панели, чтобы обычный UI читал кэш из `SQLite`, а не делал прямой live-запрос.

## Таблица `access_events`

- `id INTEGER PRIMARY KEY`
- `panel_id INTEGER NOT NULL`
- `event_time TEXT NOT NULL`
- `employee_id TEXT`
- `employee_db_id INTEGER`
- `employee_name_snapshot TEXT`
- `room_number_snapshot TEXT`
- `card_number_snapshot TEXT`
- `event_kind TEXT NOT NULL`
- `result TEXT NOT NULL`
- `unlock_method TEXT NOT NULL`
- `snapshot_path TEXT`
- `raw_json TEXT NOT NULL`
- `created_at TEXT NOT NULL`

## Таблица `sync_runs`

- `id INTEGER PRIMARY KEY`
- `started_at TEXT NOT NULL`
- `finished_at TEXT`
- `status TEXT NOT NULL`
- `summary_json TEXT`

## Таблица `sync_actions`

- `id INTEGER PRIMARY KEY`
- `sync_run_id INTEGER NOT NULL`
- `panel_id INTEGER NOT NULL`
- `employee_id TEXT`
- `action TEXT NOT NULL`
- `status TEXT NOT NULL`
- `message TEXT`
- `created_at TEXT NOT NULL`

## Индексы

В проекте используются индексы как минимум для:

- `employees(employee_id)`
- `employees(card_number)`
- `access_events(event_time)`
- `access_events(employee_id)`
- `access_events(employee_db_id)`
- `access_events(panel_id)`
- `access_events(result)`
- `access_events(unlock_method)`
- `sync_actions(sync_run_id)`


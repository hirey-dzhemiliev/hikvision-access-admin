# Конфигурация и настройки

## Структура YAML

Основные секции:

- `auth`
- `server`
- `storage`
- `retention`
- `journal`
- `logging`

## `auth`

- `secret_key`
- список пользователей админки

Для каждого пользователя:

- `username`
- `password_hash`
- `display_name`
- `is_active`

Авторизация session-based, после входа создаётся cookie-сессия.

## `server`

- `host`
- `port`
- `threads`
- `timezone`
- `panel_health_interval_minutes`

Этот параметр сейчас используется как срок свежести кэша статусов панели и diff синхронизации.

## `storage`

- `db_path`
- `employee_media_dir`
- `event_media_dir`
- `bootstrap_panels_json`

## `retention`

- `event_retention_days`
- `snapshot_retention_days`
- `delete_events_enabled`
- `delete_snapshots_enabled`
- `cleanup_interval_minutes`

## `journal`

- `save_snapshots_for_granted`
- `save_snapshots_for_denied`
- `events_per_page`
- `show_unmatched_events`

## `logging`

- `file`
- `level`
- `max_bytes`
- `backup_count`

## Страница настроек

В интерфейсе есть отдельная страница `Настройки`, где можно редактировать:

- политику хранения журнала;
- сохранение фото;
- отображение журнала;
- служебные параметры интерфейса.


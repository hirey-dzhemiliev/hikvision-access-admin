# Запуск и деплой

## Локальный запуск

```bash
python3 -m pip install -r requirements.txt
cp config.yaml.example config.yaml
python3 hikvision_admin_app.py --config config.yaml
```

После запуска приложение обычно доступно по адресу:

- `http://127.0.0.1:8080`

## Генерация хеша пароля

```bash
python3 hikvision_admin_app.py --hash-password "new-password"
```

Полученный хеш нужно записать в `auth.users[].password_hash`.

## Docker

В проекте подготовлены:

- `Dockerfile`
- `docker-compose.yml`
- `config.docker.yaml`

Быстрый запуск:

```bash
docker compose up -d --build
```

По умолчанию compose:

- публикует `8080:8080`;
- монтирует `./data -> /data`;
- монтирует `./config.docker.yaml -> /app/config.docker.yaml`.

Если нужен первичный bootstrap панелей, можно положить файл в:

- `./data/panels.json`

## Продакшен-заметки

- основной production WSGI-сервер: `waitress`
- при отсутствии `waitress` приложение временно откатывается на `wsgiref` только для локального тестирования
- приложение пишет логи в stdout/stderr и в ротационный лог-файл
- перед продакшен-запуском нужно заменить `secret_key` и пароль администратора


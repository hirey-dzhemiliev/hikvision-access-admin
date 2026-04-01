FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

RUN mkdir -p /data /data/media/employees /data/media/events /data/logs

EXPOSE 8080

CMD ["python3", "hikvision_admin_app.py", "--config", "/app/config.docker.yaml"]

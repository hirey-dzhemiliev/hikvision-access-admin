#!/usr/bin/env python3
"""
Batch user provisioning for multiple Hikvision panels.

Default endpoint is based on Hikvision ISAPI access control docs:
PUT /ISAPI/AccessControl/UserInfo/SetUp?format=json
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import mimetypes
import os
import re
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any


DEFAULT_ENDPOINT = "/ISAPI/AccessControl/UserInfo/SetUp?format=json"
DEFAULT_FACE_ENDPOINT = "/ISAPI/Intelligent/FDLib/FaceDataRecord?format=json"
DEFAULT_EVENT_ENDPOINT = "/ISAPI/Event/notification/alertStream"
DEFAULT_TIMEOUT = 15

# Firmware error string constants used in API error messages
ERR_NOT_SUPPORT = "notSupport"
ERR_METHOD_NOT_ALLOWED = "methodNotAllowed"
ERR_CARD_ALREADY_EXIST = "cardAlreadyExist"
ERR_DEVICE_USER_ALREADY_EXIST_FACE = "deviceUserAlreadyExistFace"


class HikvisionApiError(Exception):
    pass


@dataclass(slots=True)
class PanelConfig:
    name: str
    host: str
    username: str
    password: str
    protocol: str = "http"
    verify_tls: bool = False
    endpoint: str = DEFAULT_ENDPOINT
    timeout: int = DEFAULT_TIMEOUT

    @property
    def base_url(self) -> str:
        return f"{self.protocol}://{self.host}"

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "PanelConfig":
        required = ["name", "host", "username", "password"]
        missing = [field for field in required if field not in raw]
        if missing:
            raise ValueError(f"Panel config missing fields: {', '.join(missing)}")
        return cls(
            name=str(raw["name"]),
            host=str(raw["host"]),
            username=str(raw["username"]),
            password=str(raw["password"]),
            protocol=str(raw.get("protocol", "http")),
            verify_tls=bool(raw.get("verify_tls", False)),
            endpoint=str(raw.get("endpoint", DEFAULT_ENDPOINT)),
            timeout=int(raw.get("timeout", DEFAULT_TIMEOUT)),
        )


def md5_hex(value: str) -> str:
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def parse_www_authenticate(header: str) -> tuple[str, dict[str, str]]:
    scheme, _, params_raw = header.partition(" ")
    pairs = re.findall(r'(\w+)=(".*?"|[^,]+)', params_raw)
    params: dict[str, str] = {}
    for key, value in pairs:
        params[key] = value.strip(' "')
    return scheme.lower(), params


def build_multipart_form_data(
    fields: dict[str, str],
    files: dict[str, str],
) -> tuple[bytes, str]:
    boundary = f"----CodexBoundary{uuid.uuid4().hex}"
    body = bytearray()

    for name, value in fields.items():
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(
            f'Content-Disposition: form-data; name="{name}"\r\n'.encode("utf-8")
        )
        body.extend(b"Content-Type: application/json\r\n\r\n")
        body.extend(value.encode("utf-8"))
        body.extend(b"\r\n")

    for name, path in files.items():
        filename = os.path.basename(path)
        mime_type = mimetypes.guess_type(path)[0] or "application/octet-stream"
        with open(path, "rb") as fh:
            data = fh.read()

        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(
            (
                f'Content-Disposition: form-data; name="{name}"; '
                f'filename="{filename}"\r\n'
            ).encode("utf-8")
        )
        body.extend(f"Content-Type: {mime_type}\r\n\r\n".encode("utf-8"))
        body.extend(data)
        body.extend(b"\r\n")

    body.extend(f"--{boundary}--\r\n".encode("utf-8"))
    return bytes(body), f"multipart/form-data; boundary={boundary}"


class HikvisionISAPIClient:
    def __init__(self, panel: PanelConfig):
        self.panel = panel
        self._nc = 0
        self._last_nonce: str = ""

    def upsert_user(self, user_payload: dict[str, Any]) -> dict[str, Any]:
        payload = {"UserInfo": user_payload}
        return self.request_json("PUT", self.panel.endpoint, payload)

    def upload_face(
        self,
        employee_no: str,
        name: str,
        photo_path: str,
        fdid: str = "1",
        face_lib_type: str = "blackFD",
        endpoint: str = DEFAULT_FACE_ENDPOINT,
    ) -> dict[str, Any]:
        metadata = {
            "faceLibType": face_lib_type,
            "FDID": fdid,
            "FPID": employee_no,
            "name": name,
        }
        body, content_type = build_multipart_form_data(
            fields={"FaceDataRecord": json.dumps(metadata, ensure_ascii=False)},
            files={"img": photo_path},
        )
        return self.request_raw(
            "POST",
            endpoint,
            body=body,
            headers={"Content-Type": content_type, "Accept": "application/json"},
        )

    def request_json(
        self,
        method: str,
        endpoint: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        return self.request_raw(method, endpoint, body=body, headers=headers)

    def request_raw(
        self,
        method: str,
        endpoint: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        headers = headers or {}
        url = urllib.parse.urljoin(self.panel.base_url, endpoint)
        request = urllib.request.Request(url, data=body, method=method, headers=headers)

        try:
            return self._open_json(request)
        except urllib.error.HTTPError as exc:
            auth_header = exc.headers.get("WWW-Authenticate", "")
            if exc.code != 401 or not auth_header:
                detail = self._safe_error_body(exc)
                raise HikvisionApiError(f"HTTP {exc.code}: {detail}") from exc

            scheme, params = parse_www_authenticate(auth_header)
            url_parts = urllib.parse.urlsplit(url)
            request_uri = url_parts.path + (f"?{url_parts.query}" if url_parts.query else "")
            if scheme == "digest":
                request.add_header(
                    "Authorization",
                    self._build_digest_header(
                        method=method,
                        uri=request_uri,
                        params=params,
                    ),
                )
                try:
                    return self._open_json(request)
                except urllib.error.HTTPError as inner_exc:
                    detail = self._safe_error_body(inner_exc)
                    raise HikvisionApiError(f"HTTP {inner_exc.code}: {detail}") from inner_exc

            if scheme == "basic":
                token = self._basic_token()
                request.add_header("Authorization", f"Basic {token}")
                try:
                    return self._open_json(request)
                except urllib.error.HTTPError as inner_exc:
                    detail = self._safe_error_body(inner_exc)
                    raise HikvisionApiError(f"HTTP {inner_exc.code}: {detail}") from inner_exc

            raise HikvisionApiError(f"Unsupported auth scheme: {scheme}")

    def _open_json(self, request: urllib.request.Request) -> dict[str, Any]:
        with urllib.request.urlopen(
            request,
            timeout=self.panel.timeout,
            context=self._ssl_context(),
        ) as response:
            raw = response.read().decode("utf-8", errors="replace").strip()
            if not raw:
                return {"ok": True}
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return {"raw": raw}

    def _ssl_context(self) -> ssl.SSLContext | None:
        if self.panel.protocol != "https":
            return None
        if self.panel.verify_tls:
            return ssl.create_default_context()
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    def _build_digest_header(self, method: str, uri: str, params: dict[str, str]) -> str:
        realm = params["realm"]
        nonce = params["nonce"]
        qop = params.get("qop", "auth").split(",")[0].strip()
        opaque = params.get("opaque")
        algorithm = params.get("algorithm", "MD5").upper()
        if algorithm != "MD5":
            raise HikvisionApiError(f"Unsupported digest algorithm: {algorithm}")

        if nonce != self._last_nonce:
            self._nc = 0
            self._last_nonce = nonce
        self._nc += 1
        nc = f"{self._nc:08x}"
        cnonce = uuid.uuid4().hex
        ha1 = md5_hex(f"{self.panel.username}:{realm}:{self.panel.password}")
        ha2 = md5_hex(f"{method}:{uri}")
        response = md5_hex(f"{ha1}:{nonce}:{nc}:{cnonce}:{qop}:{ha2}")

        parts = [
            f'username="{self.panel.username}"',
            f'realm="{realm}"',
            f'nonce="{nonce}"',
            f'uri="{uri}"',
            f'response="{response}"',
            f'algorithm="{algorithm}"',
            f"qop={qop}",
            f"nc={nc}",
            f'cnonce="{cnonce}"',
        ]
        if opaque:
            parts.append(f'opaque="{opaque}"')
        return "Digest " + ", ".join(parts)

    def _basic_token(self) -> str:
        raw = f"{self.panel.username}:{self.panel.password}".encode("utf-8")
        return base64.b64encode(raw).decode("ascii")

    def _safe_error_body(self, exc: urllib.error.HTTPError) -> str:
        try:
            body = exc.read().decode("utf-8", errors="replace").strip()
        except Exception:
            body = ""
        return body or exc.reason or "request failed"


class MultiPanelProvisioner:
    def __init__(self, panels: list[PanelConfig]):
        self.panels = panels

    def add_user_to_all(self, user_payload: dict[str, Any]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for panel in self.panels:
            client = HikvisionISAPIClient(panel)
            try:
                response = client.upsert_user(user_payload)
                results.append(
                    {
                        "panel": panel.name,
                        "host": panel.host,
                        "ok": True,
                        "response": response,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                results.append(
                    {
                        "panel": panel.name,
                        "host": panel.host,
                        "ok": False,
                        "error": str(exc),
                    }
                )
        return results

    def upload_face_to_all(
        self,
        employee_no: str,
        name: str,
        photo_path: str,
        fdid: str = "1",
        face_lib_type: str = "blackFD",
        endpoint: str = DEFAULT_FACE_ENDPOINT,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for panel in self.panels:
            client = HikvisionISAPIClient(panel)
            try:
                response = client.upload_face(
                    employee_no=employee_no,
                    name=name,
                    photo_path=photo_path,
                    fdid=fdid,
                    face_lib_type=face_lib_type,
                    endpoint=endpoint,
                )
                results.append(
                    {
                        "panel": panel.name,
                        "host": panel.host,
                        "ok": True,
                        "response": response,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                results.append(
                    {
                        "panel": panel.name,
                        "host": panel.host,
                        "ok": False,
                        "error": str(exc),
                    }
                )
        return results


class HikvisionEventStreamClient(HikvisionISAPIClient):
    """Client that opens a persistent multipart alertStream connection."""

    def open_stream(self, endpoint: str = DEFAULT_EVENT_ENDPOINT):
        url = urllib.parse.urljoin(self.panel.base_url, endpoint)
        request = urllib.request.Request(url, method="GET")
        try:
            return urllib.request.urlopen(
                request,
                timeout=self.panel.timeout,
                context=self._ssl_context(),
            )
        except urllib.error.HTTPError as exc:
            auth_header = exc.headers.get("WWW-Authenticate", "")
            if exc.code != 401 or not auth_header:
                detail = self._safe_error_body(exc)
                raise HikvisionApiError(f"HTTP {exc.code}: {detail}") from exc
            scheme, params = parse_www_authenticate(auth_header)
            url_parts = urllib.parse.urlsplit(url)
            request_uri = url_parts.path + (f"?{url_parts.query}" if url_parts.query else "")
            if scheme == "digest":
                request.add_header(
                    "Authorization",
                    self._build_digest_header("GET", request_uri, params),
                )
            elif scheme == "basic":
                request.add_header("Authorization", f"Basic {self._basic_token()}")
            else:
                raise HikvisionApiError(f"Unsupported auth scheme: {scheme}")
            return urllib.request.urlopen(
                request,
                timeout=self.panel.timeout,
                context=self._ssl_context(),
            )


class MultipartStreamParser:
    """Parses an RFC-2046 multipart/mixed response stream, yielding (headers, body) pairs."""

    def __init__(self, response):
        content_type = response.headers.get("Content-Type", "")
        marker = "boundary="
        if marker not in content_type:
            raise ValueError(f"Unexpected stream content type: {content_type}")
        boundary = content_type.split(marker, 1)[1].strip().strip('"')
        self.boundary = f"--{boundary}".encode("utf-8")
        self.response = response

    def parts(self):
        stream = self.response
        while True:
            line = stream.readline()
            if not line:
                return
            line = line.rstrip(b"\r\n")
            if not line or line == self.boundary + b"--":
                continue
            if line != self.boundary:
                continue
            headers: dict[str, str] = {}
            while True:
                header_line = stream.readline()
                if not header_line:
                    return
                header_line = header_line.rstrip(b"\r\n")
                if not header_line:
                    break
                if b":" not in header_line:
                    continue
                key, value = header_line.decode("utf-8", errors="replace").split(":", 1)
                headers[key.strip().lower()] = value.strip()
            length = int(headers.get("content-length", "0"))
            body = stream.read(length) if length else b""
            stream.read(2)
            yield headers, body


def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def load_panels(path: str) -> list[PanelConfig]:
    raw = load_json(path)
    if not isinstance(raw, list):
        raise ValueError("Panels file must contain a JSON array")
    return [PanelConfig.from_dict(item) for item in raw]


def build_user_payload(args: argparse.Namespace) -> dict[str, Any]:
    if args.user_file:
        payload = load_json(args.user_file)
        if not isinstance(payload, dict):
            raise ValueError("User file must contain a JSON object")
        return payload

    if not args.employee_no or not args.name:
        raise ValueError("Pass --user-file or both --employee-no and --name")

    payload: dict[str, Any] = {
        "employeeNo": args.employee_no,
        "name": args.name,
        "userType": args.user_type,
        "closeDelayEnabled": False,
    }

    if args.begin_time and args.end_time:
        payload["Valid"] = {
            "enable": True,
            "beginTime": args.begin_time,
            "endTime": args.end_time,
            "timeType": "local",
        }

    if args.room_no:
        payload["roomNo"] = args.room_no
    if args.floor_no:
        payload["floorNo"] = args.floor_no
    if args.local_ui_right:
        payload["localUIRight"] = True

    return payload


def print_results(results: list[dict[str, Any]]) -> int:
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0 if all(item["ok"] for item in results) else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add or update the same user on multiple Hikvision panels."
    )
    parser.add_argument("--panels-file", required=True, help="Path to panels JSON config")
    parser.add_argument("--user-file", help="Path to raw UserInfo JSON object")
    parser.add_argument("--employee-no", help="Employee/person ID")
    parser.add_argument("--name", help="User full name")
    parser.add_argument("--user-type", default="normal", help="normal, visitor, blackList")
    parser.add_argument("--begin-time", help="Example: 2026-03-26T00:00:00+03:00")
    parser.add_argument("--end-time", help="Example: 2030-12-31T23:59:59+03:00")
    parser.add_argument("--room-no", help="Optional room number")
    parser.add_argument("--floor-no", help="Optional floor number")
    parser.add_argument("--photo-file", help="Path to user face photo to upload")
    parser.add_argument("--fdid", default="1", help="Face library ID, usually 1")
    parser.add_argument(
        "--face-lib-type",
        default="blackFD",
        help="Face library type reported by device capabilities",
    )
    parser.add_argument(
        "--face-endpoint",
        default=DEFAULT_FACE_ENDPOINT,
        help="Face upload endpoint if your firmware uses a different one",
    )
    parser.add_argument(
        "--local-ui-right",
        action="store_true",
        help="Grant local UI right if your firmware supports it",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    panels = load_panels(args.panels_file)
    provisioner = MultiPanelProvisioner(panels)
    if args.photo_file:
        employee_no = args.employee_no
        name = args.name
        if args.user_file:
            payload = load_json(args.user_file)
            employee_no = employee_no or str(payload.get("employeeNo", ""))
            name = name or str(payload.get("name", ""))
        if not employee_no or not name:
            raise ValueError(
                "For face upload pass --photo-file and either --user-file or both --employee-no and --name"
            )
        return print_results(
            provisioner.upload_face_to_all(
                employee_no=employee_no,
                name=name,
                photo_path=args.photo_file,
                fdid=args.fdid,
                face_lib_type=args.face_lib_type,
                endpoint=args.face_endpoint,
            )
        )

    user_payload = build_user_payload(args)
    return print_results(provisioner.add_user_to_all(user_payload))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)

"""Bootstrap Dremio headlessly: create the first admin user and add the MinIO
source, so you only have to log in and query.

Run from the host once Dremio's UI (http://localhost:9047) is reachable:
    py scripts/dremio_setup.py

Idempotent-ish: skips steps that report "already exists".
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

BASE = "http://localhost:9047"
USER = "admin"
PASSWORD = "dremio123"


def req(method: str, path: str, body: dict | None = None, headers: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method)
    r.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        r.add_header(k, v)
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        return {"_error": e.code, "_body": e.read().decode()[:300]}


def create_first_user() -> None:
    body = {
        "userName": USER, "firstName": "Admin", "lastName": "User",
        "email": "admin@example.com", "createdAt": int(time.time() * 1000),
        "password": PASSWORD,
    }
    out = req("PUT", "/apiv2/bootstrap/firstuser", body, {"Authorization": "_dremionull"})
    if out.get("_error"):
        print(f"first user: skipped/exists ({out['_error']})")
    else:
        print("first user: created (admin / dremio123)")


def login() -> str:
    out = req("POST", "/apiv2/login", {"userName": USER, "password": PASSWORD})
    token = out.get("token")
    if not token:
        raise SystemExit(f"login failed: {out}")
    return "_dremio" + token


def add_minio_source(auth: str) -> None:
    source = {
        "entityType": "source",
        "type": "S3",
        "name": "lake",
        "config": {
            "credentialType": "ACCESS_KEY",
            "accessKey": "admin",
            "accessSecret": "password",
            "secure": False,
            "compatibilityMode": True,
            "rootPath": "/",
            "propertyList": [
                {"name": "fs.s3a.endpoint", "value": "minio:9000"},
                {"name": "fs.s3a.path.style.access", "value": "true"},
                {"name": "dremio.s3.compat", "value": "true"},
            ],
        },
    }
    out = req("POST", "/api/v3/catalog", source, {"Authorization": auth})
    if out.get("_error"):
        print(f"add source 'lake': {out['_error']} {out.get('_body','')}")
    else:
        print("add source 'lake': ok (MinIO warehouse bucket)")


if __name__ == "__main__":
    create_first_user()
    auth = login()
    add_minio_source(auth)
    print("done — open http://localhost:9047 (admin / dremio123) and query lake.coldchain.telemetry")

#!/usr/bin/env python3
import os
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv(override=False)


def require_env(name):
    value = os.getenv(name)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


DREMIO_HOST = require_env("DREMIO_HOST")
DREMIO_PORT = int(require_env("DREMIO_PORT"))
BASE_URL = f"http://{DREMIO_HOST}:{DREMIO_PORT}"

DREMIO_ADMIN_USER = require_env("DREMIO_ADMIN_USER")
DREMIO_ADMIN_PASSWORD = require_env("DREMIO_ADMIN_PASSWORD")
DREMIO_ADMIN_FIRSTNAME = require_env("DREMIO_ADMIN_FIRSTNAME")
DREMIO_ADMIN_LASTNAME = require_env("DREMIO_ADMIN_LASTNAME")
DREMIO_ADMIN_EMAIL = require_env("DREMIO_ADMIN_EMAIL")

POSTGRES_HOST = require_env("POSTGRES_HOST")
POSTGRES_PORT = int(require_env("POSTGRES_PORT"))
POSTGRES_DB = require_env("POSTGRES_DB")
POSTGRES_USER = require_env("POSTGRES_USER")
POSTGRES_PASSWORD = require_env("POSTGRES_PASSWORD")

POSTGRES_SOURCE_NAME = require_env("DREMIO_POSTGRES_SOURCE_NAME")


def log(msg):
    print(f"[dremio-init] {msg}", flush=True)


def auth_headers(token):
    return {"Authorization": f"_dremio{token}", "Content-Type": "application/json"}


def wait_for_dremio():
    log(f"Waiting for Dremio at {BASE_URL}...")
    for _ in range(30):
        try:
            r = requests.get(f"{BASE_URL}/apiv2/server_status", timeout=5)
            if r.status_code == 200:
                log("Dremio is ready!")
                return
        except Exception:
            pass
        time.sleep(10)
    log("ERROR: Dremio never became ready.")
    sys.exit(1)


def try_login():
    try:
        r = requests.post(
            f"{BASE_URL}/apiv2/login",
            json={"userName": DREMIO_ADMIN_USER, "password": DREMIO_ADMIN_PASSWORD},
            timeout=10,
        )
        if r.status_code == 200:
            return r.json()["token"]
    except Exception:
        pass
    return None


def bootstrap_first_user():
    payload = {
        "userName": DREMIO_ADMIN_USER,
        "password": DREMIO_ADMIN_PASSWORD,
        "firstName": DREMIO_ADMIN_FIRSTNAME,
        "lastName": DREMIO_ADMIN_LASTNAME,
        "email": DREMIO_ADMIN_EMAIL,
    }
    try:
        r = requests.put(
            f"{BASE_URL}/apiv2/bootstrap/firstuser", json=payload, timeout=10
        )
        if r.status_code in (200, 201):
            log(f"Bootstrapped first Dremio user '{DREMIO_ADMIN_USER}'")
            return True
        log(f"Bootstrap first user failed: {r.status_code} - {r.text[:200]}")
        return False
    except Exception as e:
        log(f"Bootstrap first user failed: {e}")
        return False


def get_token():
    token = try_login()
    if token:
        log(f"Authenticated as '{DREMIO_ADMIN_USER}'")
        return token

    log(
        f"Authentication failed for '{DREMIO_ADMIN_USER}', attempting first-user bootstrap..."
    )
    if bootstrap_first_user():
        token = try_login()
        if token:
            log(f"Authenticated as '{DREMIO_ADMIN_USER}' after bootstrap")
            return token

    log(
        f"ERROR: Authentication failed for '{DREMIO_ADMIN_USER}'. "
        "If a different admin already exists, set DREMIO_ADMIN_USER/DREMIO_ADMIN_PASSWORD (or DREMIO_USER/DREMIO_PASSWORD)."
    )
    sys.exit(1)


def delete_source_if_exists(token, name):
    try:
        r = requests.get(
            f"{BASE_URL}/api/v3/catalog/by-path/{name}",
            headers=auth_headers(token),
            timeout=10,
        )
        if r.status_code == 200:
            source_id = r.json()["id"]
            log(f"Deleting existing source '{name}'...")
            requests.delete(
                f"{BASE_URL}/api/v3/catalog/{source_id}",
                headers=auth_headers(token),
                timeout=30,
            )
    except Exception:
        pass


def create_postgres_source(token):
    log(f"Creating PostgreSQL source '{POSTGRES_SOURCE_NAME}'...")
    pg_payload = {
        "entityType": "source",
        "name": POSTGRES_SOURCE_NAME,
        "type": "POSTGRES",
        "config": {
            "hostname": POSTGRES_HOST,
            "port": str(POSTGRES_PORT),
            "databaseName": POSTGRES_DB,
            "username": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
            "useSsl": False,
        },
    }

    r = requests.post(
        f"{BASE_URL}/api/v3/catalog",
        headers=auth_headers(token),
        json=pg_payload,
        timeout=30,
    )
    if r.status_code in (200, 201):
        log("  Created PostgreSQL source")
        return True

    if r.status_code == 409:
        log("  PostgreSQL source already exists")
        return True

    log(f"  Failed: {r.status_code} - {r.text[:200]}")
    return False


def main():
    log("=" * 40)
    log("  Dremio Setup Starting")
    log("=" * 40)

    wait_for_dremio()
    token = get_token()

    success = create_postgres_source(token)
    if not success:
        log("ERROR: Failed to create PostgreSQL source")
        sys.exit(1)

    log("=" * 40)
    log("  Setup Complete!")
    log("  Dremio UI: http://localhost:9047")
    log(f"  Login: {DREMIO_ADMIN_USER} / {DREMIO_ADMIN_PASSWORD}")
    log("=" * 40)


if __name__ == "__main__":
    main()

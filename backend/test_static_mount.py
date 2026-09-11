"""Regression test for the 2026-09-11 exposure.

/static used to mount the whole project root, which published .env (every
secret), .git, the SQLite databases, their backups and the recorded
elicitation audio to the internet. Only css/, js/ and admin-tests.html may be
served there. Runs the real app in-process; no database or network needed.
"""
import os
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).parent))
# Import-time requirements only; nothing here talks to a database.
os.environ.setdefault("MOODLE_DB_TYPE", "mysql")
os.environ.setdefault("MOODLE_JWT_SECRET", "test-only-not-a-real-secret")

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def client():
    from main import app
    return TestClient(app)  # no context manager: startup hooks (DB) do not run


@pytest.mark.parametrize("path", [
    "/static/css/styles.css",
    "/static/js/app.js",
    "/static/admin-tests.html",
])
def test_frontend_assets_are_served(client, path):
    assert client.get(path).status_code == 200


def _first_audio_file():
    audio = PROJECT_ROOT / "data" / "audio"
    files = sorted(audio.iterdir()) if audio.is_dir() else []
    return f"/static/data/audio/{files[0].name}" if files else "/static/data/audio/x.wav"


@pytest.mark.parametrize("path", [
    "/static/.env",
    "/static/.env.example",
    "/static/.git/config",
    "/static/.git/HEAD",
    "/static/data/annotations.db",
    _first_audio_file(),
    "/static/database_backups/",
    "/static/elicitations_db/annotations.json",
    "/static/backend/config.py",
    "/static/CLAUDE.md",
    "/static/index.html",
    # traversal out of an allowed folder
    "/static/css/../.env",
    "/static/js/%2e%2e/.env",
    "/static/css/..%2f.env",
])
def test_project_files_are_not_served(client, path):
    assert client.get(path).status_code == 404

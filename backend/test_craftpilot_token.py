"""The CraftPilot internal token must reach CraftPilot under the name .env uses.

.env (and .env.example) define CRAFTPILOT_INTERNAL_TOKEN, but main.py read
INTERNAL_API_TOKEN, which nothing set: every annotation push and project
resync went to CraftPilot with an empty X-Internal-Token and was refused, and
/api/export/corpus refused every caller. No database or network is used.
"""
import importlib
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from starlette.requests import Request

sys.path.insert(0, str(Path(__file__).parent))
os.environ.setdefault("MOODLE_DB_TYPE", "mysql")
os.environ.setdefault("MOODLE_JWT_SECRET", "test-only-not-a-real-secret")

import config  # noqa: E402
import main  # noqa: E402


@pytest.fixture
def restore_config():
    yield
    importlib.reload(config)


def test_config_reads_the_name_env_defines(monkeypatch, restore_config):
    monkeypatch.setenv("CRAFTPILOT_INTERNAL_TOKEN", "from-documented-name")
    monkeypatch.delenv("INTERNAL_API_TOKEN", raising=False)
    assert importlib.reload(config).CRAFTPILOT_INTERNAL_TOKEN == "from-documented-name"


def test_config_still_accepts_the_old_name(monkeypatch, restore_config):
    monkeypatch.delenv("CRAFTPILOT_INTERNAL_TOKEN", raising=False)
    monkeypatch.setenv("INTERNAL_API_TOKEN", "from-old-name")
    assert importlib.reload(config).CRAFTPILOT_INTERNAL_TOKEN == "from-old-name"


async def test_annotation_push_sends_the_configured_token(monkeypatch):
    monkeypatch.setattr(main, "CRAFTPILOT_INTERNAL_TOKEN", "tok-123")

    @asynccontextmanager
    async def fake_session():
        yield object()

    monkeypatch.setattr(main.db, "AsyncSessionLocal", fake_session)
    monkeypatch.setattr(main.db, "get_annotation", AsyncMock(return_value={"video_id": 7, "start_time": 1, "end_time": 2}))
    monkeypatch.setattr(main.db, "get_video", AsyncMock(return_value={"filename": "v.mp4", "project_id": None}))

    sent = {}

    class FakeResponse:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def fake_urlopen(req, timeout=None):
        sent["token"] = req.get_header("X-internal-token")
        return FakeResponse()

    with patch("urllib.request.urlopen", fake_urlopen):
        await main.push_annotation_to_rag(42, "une transcription")
    assert sent["token"] == "tok-123"


def _request(headers):
    return Request({"type": "http", "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()]})


def test_corpus_export_guard_accepts_the_configured_token(monkeypatch):
    monkeypatch.setattr(main, "CRAFTPILOT_INTERNAL_TOKEN", "tok-123")
    main._verify_internal_token(_request({"X-Internal-Token": "tok-123"}))  # no exception


@pytest.mark.parametrize("configured, sent", [("tok-123", "wrong"), ("tok-123", ""), ("", "")])
def test_corpus_export_guard_refuses_everything_else(monkeypatch, configured, sent):
    monkeypatch.setattr(main, "CRAFTPILOT_INTERNAL_TOKEN", configured)
    with pytest.raises(HTTPException) as exc:
        main._verify_internal_token(_request({"X-Internal-Token": sent}))
    assert exc.value.status_code == 403

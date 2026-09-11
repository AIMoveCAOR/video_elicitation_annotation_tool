"""
Configuration settings for Video Elicitation Annotation Tool
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
try:
    load_dotenv()
except PermissionError:
    # .env file may have restricted permissions; proceed with defaults
    pass

# Base directories
BASE_DIR = Path(__file__).resolve().parent.parent
CHROMA_DIR = BASE_DIR / "chroma_langchain_db"
ELICITATION_DIR = CHROMA_DIR / "elicitations_db"
DATA_DIR = BASE_DIR / "data"
# VIDEOS_DIR can be overridden via env var to store uploads outside the install dir
VIDEOS_DIR = Path(os.getenv("VIDEOS_DIR", str(DATA_DIR / "videos")))
AUDIO_DIR = DATA_DIR / "audio"
EXPORTS_DIR = DATA_DIR / "exports"
FRONTEND_DIR = BASE_DIR  # Static files (css/, js/, index.html) are at project root

# Ensure directories exist
for directory in [DATA_DIR, VIDEOS_DIR, AUDIO_DIR, EXPORTS_DIR, CHROMA_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Database (keep in CHROMA_DIR root so migrate_db and app agree)
DATABASE_URL = f"sqlite+aiosqlite:///{CHROMA_DIR / 'annotations.db'}"

# === MOODLE INTEGRATION CONFIGURATION ===
MOODLE_INTEGRATION = os.getenv('MOODLE_INTEGRATION', 'false').lower() in ('true', '1', 'yes')
MOODLE_JWT_SECRET = os.getenv('MOODLE_JWT_SECRET', '')
if not MOODLE_JWT_SECRET or MOODLE_JWT_SECRET == 'changeme':
    import warnings
    warnings.warn(
        "MOODLE_JWT_SECRET is not set or is using the insecure default 'changeme'. "
        "Set a strong random secret in the .env file before accepting real traffic.",
        stacklevel=1,
    )
MOODLE_PROXY_MODE = os.getenv('MOODLE_PROXY_MODE', 'false').lower() in ('true', '1', 'yes')
MOODLE_ORIGIN = os.getenv('MOODLE_ORIGIN', '')

# === MOODLE DATABASE CONFIGURATION (NEW) ===
# Replaces SQLite for operational data
MOODLE_DB_TYPE = os.getenv('MOODLE_DB_TYPE', 'mysql')  # or 'postgresql'
MOODLE_DB_HOST = os.getenv('MOODLE_DB_HOST', 'localhost')
MOODLE_DB_PORT = int(os.getenv('MOODLE_DB_PORT', '3306'))
MOODLE_DB_NAME = os.getenv('MOODLE_DB_NAME', 'moodle')
MOODLE_DB_USER = os.getenv('MOODLE_DB_USER', 'moodleuser')
MOODLE_DB_PASSWORD = os.getenv('MOODLE_DB_PASSWORD', '')
MOODLE_TABLE_PREFIX = os.getenv('MOODLE_TABLE_PREFIX', 'mdl_')

# Server settings
HOST = "localhost"
PORT = 8005
DEBUG = os.getenv('DEBUG', 'false').lower() in ('true', '1', 'yes')

# CORS settings — includes the Moodle origin so the iframe can make same-origin API calls
_extra_origins = [o.strip() for o in os.getenv("CORS_EXTRA_ORIGINS", "").split(",") if o.strip()]
CORS_ORIGINS = [o for o in [
    "http://localhost:8005",
    "http://127.0.0.1:8005",
    MOODLE_ORIGIN,          # e.g. https://aimove.minesparis.psl.eu
    *_extra_origins,
] if o]

# Admin test runner — sent as Authorization: Bearer header (fetch) or
# exchanged for a short-lived SSE token via POST /api/admin/token.
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "")

# Shared secret for calls between this backend and CraftPilot, sent and checked
# as X-Internal-Token. .env names it CRAFTPILOT_INTERNAL_TOKEN (see .env.example);
# main.py used to read INTERNAL_API_TOKEN instead, which .env never set, so every
# push to CraftPilot went out with an empty token and was refused (HTTP 401).
CRAFTPILOT_INTERNAL_TOKEN = os.getenv("CRAFTPILOT_INTERNAL_TOKEN") or os.getenv("INTERNAL_API_TOKEN", "")

# Infomaniak AI — OpenAI-compatible, used for all active LLM inference
INFOMANIAK_API_KEY = os.getenv("INFOMANIAK_API_KEY", "")
INFOMANIAK_PRODUCT_ID = os.getenv("INFOMANIAK_PRODUCT_ID", "")
INFOMANIAK_LLM_API_URL = (
    f"https://api.infomaniak.com/2/ai/{INFOMANIAK_PRODUCT_ID}/openai/v1/chat/completions"
)
INFOMANIAK_LLM_MODEL = "swiss-ai/Apertus-70B-Instruct-2509"

# Session advisory ("Analyze my session") — analysis model + a separate, smaller
# guard model that reviews the analysis output before it reaches the user.
INFOMANIAK_ADVISORY_MODEL = os.getenv("INFOMANIAK_ADVISORY_MODEL", INFOMANIAK_LLM_MODEL)
INFOMANIAK_GUARD_MODEL = os.getenv(
    "INFOMANIAK_GUARD_MODEL", "mistralai/Ministral-3-14B-Instruct-2512"
)

# Infomaniak STT (Whisper) — async batch API, note /1/ prefix (different from LLM /2/)
INFOMANIAK_STT_API_URL = (
    f"https://api.infomaniak.com/1/ai/{INFOMANIAK_PRODUCT_ID}/openai/audio/transcriptions"
)
INFOMANIAK_RESULTS_BASE_URL = (
    f"https://api.infomaniak.com/1/ai/{INFOMANIAK_PRODUCT_ID}/results"
)
INFOMANIAK_STT_MODEL = "whisper"

# Google Drive API settings
GOOGLE_DRIVE_API_KEY = os.getenv(
    "GOOGLE_DRIVE_API_KEY", ""
)  # Optional: for accessing public folders
GOOGLE_DRIVE_DEFAULT_FOLDER_ID = os.getenv(
    "GOOGLE_DRIVE_FOLDER_ID", ""
)  # Optional: default folder ID

# Audio recording settings
AUDIO_SAMPLE_RATE = 16000  # Whisper works best with 16kHz
AUDIO_CHANNELS = 1  # Mono
AUDIO_FORMAT = "wav"

# Video settings
SUPPORTED_VIDEO_FORMATS = [".mp4", ".webm", ".ogg", ".avi", ".mov"]
MAX_VIDEO_SIZE_MB = 10000  # 10GB max video size

# File upload settings
MAX_UPLOAD_SIZE = MAX_VIDEO_SIZE_MB * 1024 * 1024  # Convert to bytes

# Export settings
EXPORT_FORMAT = "json"  # json or csv
INCLUDE_AUDIO_IN_EXPORT = True

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Frontend static files
STATIC_DIR = FRONTEND_DIR

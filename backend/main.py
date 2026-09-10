"""
FastAPI main application for Video Elicitation Annotation Tool
"""

import os
import base64
import json
import logging
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, cast
import uuid

from pymysql import IntegrityError

from fastapi import (
    FastAPI,
    File,
    UploadFile,
    Depends,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
    Request,
    Form,
)
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import aiofiles
from starlette.types import Scope

# Use absolute imports to allow running main.py directly
# MIGRATED: Using Moodle database instead of SQLite
import database_compat as db
from auth import verify_moodle_jwt, require_capability, MoodleUser
from database_compat import AsyncSession  # Type alias for compatibility
import models
from moodle_db import moodle_db
from models import Task
import httpx
import time
from collections import defaultdict
from threading import Lock

from transcription import transcribe_audio, preload_model, get_model_info
from config import (
    HOST,
    PORT,
    CORS_ORIGINS,
    VIDEOS_DIR,
    AUDIO_DIR,
    EXPORTS_DIR,
    STATIC_DIR,
    FRONTEND_DIR,
    SUPPORTED_VIDEO_FORMATS,
    MAX_UPLOAD_SIZE,
    GOOGLE_DRIVE_API_KEY,
    GOOGLE_DRIVE_DEFAULT_FOLDER_ID,
)


# ---------------------------------------------------------------------------
# In-process sliding-window rate limiter
# ---------------------------------------------------------------------------

class _SlidingWindowRateLimiter:
    """Thread-safe per-key sliding window rate limiter (no external dependencies)."""

    def __init__(self) -> None:
        self._windows: dict[str, list[float]] = defaultdict(list)
        self._lock = Lock()

    def check(self, key: str, max_requests: int, window_seconds: int) -> tuple[bool, int]:
        """Return (allowed, retry_after_seconds). allowed=False → 429."""
        now = time.monotonic()
        cutoff = now - window_seconds
        with self._lock:
            hits = [t for t in self._windows[key] if t > cutoff]
            self._windows[key] = hits
            if len(hits) >= max_requests:
                retry_after = int(hits[0] - cutoff) + 1
                return False, retry_after
            self._windows[key].append(now)
            return True, 0


_rate_limiter = _SlidingWindowRateLimiter()


def _enforce_rate_limit(user_key: str, max_requests: int, window_seconds: int) -> None:
    """Raise HTTP 429 with Retry-After header if rate limit exceeded."""
    allowed, retry_after = _rate_limiter.check(user_key, max_requests, window_seconds)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded — try again in {retry_after}s.",
            headers={"Retry-After": str(retry_after)},
        )


# ---------------------------------------------------------------------------
# Stream ticket helper
# ---------------------------------------------------------------------------

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Video Elicitation Annotation Tool",
    description="Tool for annotating expert craftsmen videos with audio elicitations",
    version="1.0.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Coverage detector endpoints (spaCy-backed, no LLM).
from coverage_routes import router as coverage_router
app.include_router(coverage_router)

# Admin endpoints — integration test runner (SSE).
from admin_routes import router as admin_router
app.include_router(admin_router)


# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(
            f"WebSocket connected. Total connections: {len(self.active_connections)}"
        )

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(
            f"WebSocket disconnected. Total connections: {len(self.active_connections)}"
        )

    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients"""
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to websocket: {e}")


manager = ConnectionManager()

_ALLOWED_AUDIO_EXTS = {".wav", ".webm", ".ogg", ".mp3", ".mp4", ".m4a"}


# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize database and preload Whisper model"""
    import tempfile
    logger.info("Starting Video Elicitation Annotation Tool...")

    # Ensure required directories exist
    VIDEOS_DIR.mkdir(parents=True, exist_ok=True)
    AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Redirect Python's tempfile to the /var partition (198 GB) so large
    # uploads don't overflow the dedicated 2 GB /tmp filesystem.
    tmp_override = Path(os.getenv("TMPDIR_OVERRIDE", "/var/video_uploads/.tmp"))
    tmp_override.mkdir(parents=True, exist_ok=True)
    tempfile.tempdir = str(tmp_override)
    os.environ["TMPDIR"] = str(tmp_override)
    logger.info(f"Temp directory set to {tmp_override}")
    logger.info("Directories verified")

    await db.init_db()
    logger.info("Database initialized")

    await moodle_db.ensure_crafts_table()
    logger.info("Custom crafts table verified")

    await moodle_db.ensure_session_advisories_table()
    logger.info("Session advisories table verified")

    # Preload Whisper model in background
    import asyncio

    asyncio.create_task(preload_model())
    logger.info("Whisper model loading in background...")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down Video Elicitation Annotation Tool...")


# WebSocket endpoint for real-time updates
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    # Broadcasts on this socket include other users' live transcription text,
    # so require the same Moodle login as everything else. WebSockets can't
    # send an Authorization header, so the token travels as ?token=... (same
    # fallback verify_moodle_jwt already supports for <video> src requests).
    token = websocket.query_params.get("token")
    try:
        verify_moodle_jwt(authorization=f"Bearer {token}" if token else None)
    except HTTPException:
        await websocket.close(code=1008)
        return

    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive and receive any client messages
            data = await websocket.receive_text()
            logger.debug(f"Received from client: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# API Endpoints


@app.get("/")
async def read_root():
    """Serve the main frontend application"""
    index_path = FRONTEND_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return {"message": "Video Elicitation Annotation Tool API"}


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "whisper_model": get_model_info(),
    }


@app.get("/api/storage-mode")
async def storage_mode():
    """Return storage mode — always 'server' now that OwnCloud has been removed"""
    return {"mode": "server"}


@app.post("/api/videos/upload", response_model=models.VideoResponse)
async def upload_video(
    file: UploadFile = File(...),
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Upload a video file"""
    # 10 uploads per user per hour — large file I/O is resource-intensive.
    _enforce_rate_limit(f"upload:{current_user.userid}", max_requests=10, window_seconds=3600)
    try:
        # Validate filename exists
        if not file.filename:
            raise HTTPException(status_code=400, detail="No filename provided")

        # Validate file type
        file_ext = Path(file.filename).suffix.lower()
        if file_ext not in SUPPORTED_VIDEO_FORMATS:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported video format. Supported: {', '.join(SUPPORTED_VIDEO_FORMATS)}",
            )

        # Generate unique filename
        unique_filename = f"{uuid.uuid4().hex}_{file.filename}"
        file_path = VIDEOS_DIR / unique_filename

        # Save file — stream in 4 MB chunks to avoid loading the whole video
        # into memory and to keep /tmp free from large spooled files.
        logger.info(f"Uploading video: {file.filename}")
        total_written = 0
        try:
            async with aiofiles.open(file_path, "wb") as f:
                while True:
                    chunk = await file.read(4 * 1024 * 1024)  # 4 MB at a time
                    if not chunk:
                        break
                    total_written += len(chunk)
                    if total_written > MAX_UPLOAD_SIZE:
                        raise HTTPException(
                            status_code=400,
                            detail=f"File too large. Maximum size: {MAX_UPLOAD_SIZE / (1024*1024)}MB",
                        )
                    await f.write(chunk)
        except HTTPException:
            if file_path.exists():
                file_path.unlink(missing_ok=True)
            raise

        # Get file size
        file_size = os.path.getsize(file_path)

        # Create database record
        video_data = models.VideoCreate(
            filename=file.filename,
            filepath=str(file_path),
            file_size=file_size,
            mime_type=file.content_type,
            user_id=current_user.userid,
        )

        video = await db.create_video(session, video_data)
        logger.info(f"Video uploaded successfully: ID={video.id}")

        # Add annotation count
        response_data = models.VideoResponse.model_validate(video)
        response_data.annotation_count = 0

        return response_data

    except IntegrityError:
        logger.warning("Duplicate video upload attempt (same filepath)")
        raise HTTPException(status_code=409, detail="Video already registered")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading video: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/videos", response_model=List[models.VideoResponse])
async def list_videos(
    skip: int = 0,
    limit: int = 100,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Get list of videos belonging to the current user"""
    try:
        videos = await db.get_videos_by_user(session, current_user.userid, skip, limit)

        # Add annotation count to each video
        response_videos = []
        for video in videos:
            video_response = models.VideoResponse.model_validate(video)
            video_response.annotation_count = await db.get_annotation_count(session, video.id)
            response_videos.append(video_response)

        return response_videos

    except Exception as e:
        logger.error(f"Error listing videos: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _authorize_video_access(video, current_user: MoodleUser, *, manage: bool = False) -> None:
    """Reject cross-user access to a video, mirroring _authorize_annotation_access."""
    owner_id = getattr(video, "user_id", None)
    if not owner_id or owner_id == current_user.userid:
        return

    required_capability = "manage" if manage else "viewall"
    if not current_user.has_capability(required_capability):
        raise HTTPException(status_code=403, detail="Not authorized to access this video")


@app.get("/api/videos/{video_id}", response_model=models.VideoResponse)
async def get_video(
    video_id: int,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Get video by ID"""
    try:
        video = await db.get_video(session, video_id)
        if not video:
            raise HTTPException(status_code=404, detail="Video not found")
        _authorize_video_access(video, current_user)

        video_response = models.VideoResponse.model_validate(video)
        video_response.annotation_count = len(video.annotations)

        return video_response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting video: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/videos/{video_id}/file")
async def get_video_file(
    video_id: int,
    request: Request,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Serve video file with HTTP Range request support for streaming.

    Auth accepts either an Authorization header or a `?token=` query param
    (see auth.verify_moodle_jwt) since <video src="..."> can't set headers.
    """
    try:
        video = await db.get_video(session, video_id)
        if not video:
            raise HTTPException(status_code=404, detail="Video not found")
        _authorize_video_access(video, current_user)

        if not os.path.exists(str(video.filepath)):
            raise HTTPException(status_code=404, detail="Video file not found on disk")

        # Get file info
        file_size = os.path.getsize(str(video.filepath))

        # Handle Range requests for streaming
        range_header = request.headers.get("range")

        if range_header:
            # Parse range header (e.g., "bytes=0-1023")
            range_match = range_header.replace("bytes=", "").split("-")
            start = int(range_match[0]) if range_match[0] else 0
            end = (
                int(range_match[1])
                if len(range_match) > 1 and range_match[1]
                else file_size - 1
            )

            # Ensure valid range
            if start >= file_size or end >= file_size:
                raise HTTPException(status_code=416, detail="Range not satisfiable")

            chunk_size = end - start + 1

            # Stream the requested chunk using async I/O to avoid blocking the event loop
            async def iter_range():
                async with aiofiles.open(str(video.filepath), "rb") as f:
                    await f.seek(start)
                    remaining = chunk_size
                    while remaining > 0:
                        read_size = min(1048576, remaining)
                        data = await f.read(read_size)
                        if not data:
                            break
                        remaining -= len(data)
                        yield data

            headers = {
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(chunk_size),
                "Content-Type": video.mime_type or "video/mp4",
            }

            return StreamingResponse(
                iter_range(), status_code=206, headers=headers  # Partial Content
            )
        else:
            # No range header - stream entire file using async I/O
            async def iter_full():
                async with aiofiles.open(str(video.filepath), "rb") as f:
                    while chunk := await f.read(1048576):
                        yield chunk

            headers = {
                "Accept-Ranges": "bytes",
                "Content-Length": str(file_size),
                "Content-Type": video.mime_type or "video/mp4",
            }

            return StreamingResponse(
                iter_full(),
                media_type=str(video.mime_type) or "video/mp4",
                headers=headers,
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error serving video file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/videos/{video_id}", response_model=models.VideoResponse)
async def update_video(
    video_id: int,
    video_update: dict,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Update video metadata (e.g., project_id, batch_position)"""
    try:
        video = await db.get_video(session, video_id)
        if not video:
            raise HTTPException(status_code=404, detail="Video not found")
        _authorize_video_access(video, current_user, manage=True)

        # Update allowed fields
        update_payload = {}
        if "project_id" in video_update:
            update_payload["project_id"] = video_update["project_id"]
        if "batch_position" in video_update:
            update_payload["batch_position"] = video_update["batch_position"]
        if "display_name" in video_update:
            new_name = video_update["display_name"]
            if isinstance(new_name, str):
                new_name = new_name.strip()
                update_payload["display_name"] = new_name if new_name else None
            elif new_name is None:
                update_payload["display_name"] = None

        video = await db.update_video(session, video_id, update_payload)
        if not video:
            raise HTTPException(status_code=404, detail="Video not found")

        video_response = models.VideoResponse.model_validate(video)
        try:
            video_response.annotation_count = await db.get_annotation_count(session, video_id)
        except Exception:
            video_response.annotation_count = 0

        logger.info(f"Video updated: ID={video_id}")
        return video_response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating video: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/videos/{video_id}")
async def delete_video(
    video_id: int,
    force: bool = False,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Delete a video and all its annotations"""
    try:
        video = await db.get_video(session, video_id)
        if not video:
            raise HTTPException(status_code=404, detail="Video not found")
        _authorize_video_access(video, current_user, manage=True)

        # Delete the local file if it exists
        try:
            fp = str(video.filepath or "")
            if fp and os.path.exists(fp) and os.path.isfile(fp):
                os.remove(fp)
            else:
                logger.debug(f"Skipping file unlink for video ID={video_id} (filepath={fp})")
        except Exception as e:
            logger.warning(f"Skipping file delete for video ID={video_id}: {e}")

        # Delete annotation audio files (only if they exist and are files)
        for annotation in video.annotations:
            try:
                afp = str(annotation.audio_filepath or "")
                if afp and os.path.exists(afp) and os.path.isfile(afp):
                    os.remove(afp)
                else:
                    logger.debug(f"Skipping audio unlink for annotation {annotation.id} (path={afp})")
            except Exception as e:
                logger.warning(f"Failed to remove audio file for annotation {annotation.id}: {e}")

        # Delete from database
        await db.delete_video(session, video_id)

        logger.info(f"Video deleted: ID={video_id}")
        return {"status": "success", "message": "Video deleted"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting video: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# VIDEO SEGMENT ENDPOINTS
# ============================================================================


@app.post("/api/segments", response_model=models.VideoSegmentResponse)
async def create_segment(
    segment_data: models.VideoSegmentCreate,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Create a new video segment"""
    try:
        # Verify parent video exists
        video = await db.get_video(session, segment_data.parent_video_id)
        if not video:
            raise HTTPException(status_code=404, detail="Parent video not found")
        _authorize_video_access(video, current_user, manage=True)

        # Validate time range
        if segment_data.start_time >= segment_data.end_time:
            raise HTTPException(status_code=400, detail="Start time must be before end time")
        
        if video.duration and segment_data.end_time > video.duration:
            raise HTTPException(status_code=400, detail="End time exceeds video duration")
        
        segment = await db.create_video_segment(session, segment_data)
        logger.info(f"Video segment created: ID={segment.id}, parent_video_id={segment.parent_video_id}")
        
        return models.VideoSegmentResponse.model_validate(segment)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating video segment: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/segments/video/{video_id}", response_model=List[models.VideoSegmentResponse])
async def list_video_segments(
    video_id: int,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """List all segments for a specific video"""
    try:
        video = await db.get_video(session, video_id)
        if not video:
            raise HTTPException(status_code=404, detail="Video not found")
        _authorize_video_access(video, current_user)

        segments = await db.get_video_segments(session, video_id)
        return [models.VideoSegmentResponse.model_validate(s) for s in segments]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing video segments: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/segments/{segment_id}", response_model=models.VideoSegmentResponse)
async def get_segment(
    segment_id: int,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Get a specific video segment"""
    try:
        segment = await db.get_video_segment(session, segment_id)
        if not segment:
            raise HTTPException(status_code=404, detail="Segment not found")
        video = await db.get_video(session, segment.parent_video_id)
        if video:
            _authorize_video_access(video, current_user)
        return models.VideoSegmentResponse.model_validate(segment)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting video segment: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/segments/{segment_id}", response_model=models.VideoSegmentResponse)
async def update_segment(
    segment_id: int,
    segment_update: models.VideoSegmentUpdate,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Update a video segment"""
    try:
        existing = await db.get_video_segment(session, segment_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Segment not found")
        video = await db.get_video(session, existing.parent_video_id)
        if video:
            _authorize_video_access(video, current_user, manage=True)

        segment = await db.update_video_segment(session, segment_id, segment_update)
        if not segment:
            raise HTTPException(status_code=404, detail="Segment not found")

        logger.info(f"Video segment updated: ID={segment_id}")
        return models.VideoSegmentResponse.model_validate(segment)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating video segment: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/segments/{segment_id}")
async def delete_segment(
    segment_id: int,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Delete a video segment"""
    try:
        segment = await db.get_video_segment(session, segment_id)
        if not segment:
            raise HTTPException(status_code=404, detail="Segment not found")
        video = await db.get_video(session, segment.parent_video_id)
        if video:
            _authorize_video_access(video, current_user, manage=True)

        # Delete thumbnail file if exists
        if segment.thumbnail_path and os.path.exists(segment.thumbnail_path):
            os.remove(segment.thumbnail_path)
        
        await db.delete_video_segment(session, segment_id)
        logger.info(f"Video segment deleted: ID={segment_id}")
        
        return {"status": "success", "message": "Segment deleted"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting video segment: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# LOCAL VIDEO ENDPOINTS
# ============================================================================


@app.get("/api/videos/local/browse")
async def browse_local_directory(
    directory: str,
    current_user: MoodleUser = Depends(require_capability("manage")),
):
    """
    Browse a local directory for video files. Restricted to staff (teacher/
    admin 'manage' capability) since this walks the server's filesystem at
    a caller-supplied path by design, to let staff import course videos
    already placed on disk.

    Args:
        directory: Absolute path to directory to browse

    Returns:
        List of video files with metadata
    """
    try:
        dir_path = Path(directory)

        # Security check - ensure directory exists and is accessible
        if not dir_path.exists():
            raise HTTPException(status_code=404, detail="Directory not found")

        if not dir_path.is_dir():
            raise HTTPException(status_code=400, detail="Path is not a directory")

        # List all video files in directory
        video_files = []

        for file_path in dir_path.iterdir():
            if file_path.is_file():
                file_ext = file_path.suffix.lower()
                if file_ext in SUPPORTED_VIDEO_FORMATS:
                    try:
                        file_size = file_path.stat().st_size
                        video_files.append(
                            {
                                "filename": file_path.name,
                                "filepath": str(file_path.absolute()),
                                "file_size": file_size,
                                "file_size_mb": round(file_size / (1024 * 1024), 2),
                            }
                        )
                    except Exception as e:
                        logger.warning(f"Could not read file {file_path}: {e}")
                        continue

        # Sort by filename
        video_files.sort(key=lambda x: x["filename"])

        logger.info(f"Found {len(video_files)} video files in {directory}")
        return {
            "directory": str(dir_path.absolute()),
            "video_count": len(video_files),
            "videos": video_files,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error browsing local directory: {e}")
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/api/videos/local/register", response_model=models.VideoResponse)
async def register_local_video(
    request: models.LocalVideoRegisterRequest,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(require_capability("manage")),
):
    """
    Register a local video file without copying it.

    Restricted to staff ('manage' capability) — same filesystem-path-import
    concern as browse_local_directory above.

    Args:
        request: Request body containing filepath

    Returns:
        Video metadata
    """
    try:
        logger.info(f"Attempting to register local video: {request.filepath}")
        file_path = Path(request.filepath)

        # Validate file exists
        if not file_path.exists():
            logger.error(
                f"File not found: {request.filepath} (resolved to: {file_path.absolute()})"
            )
            raise HTTPException(
                status_code=404,
                detail=f"Video file not found at path: {request.filepath}",
            )

        if not file_path.is_file():
            logger.error(f"Path is not a file: {request.filepath}")
            raise HTTPException(status_code=400, detail="Path is not a file")

        # Validate file type
        file_ext = file_path.suffix.lower()
        if file_ext not in SUPPORTED_VIDEO_FORMATS:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported video format. Supported: {', '.join(SUPPORTED_VIDEO_FORMATS)}",
            )

        # Get file metadata
        file_size = file_path.stat().st_size

        # Determine MIME type
        mime_type = "video/mp4"  # Default
        if file_ext == ".webm":
            mime_type = "video/webm"
        elif file_ext == ".mov":
            mime_type = "video/quicktime"
        elif file_ext == ".avi":
            mime_type = "video/x-msvideo"

        # Create database record (no file copying)
        video_data = models.VideoCreate(
            filename=file_path.name,
            filepath=str(file_path.absolute()),
            file_size=file_size,
            mime_type=mime_type,
            is_local=1,
            source_type="local",
            user_id=current_user.userid,
        )

        video = await db.create_video(session, video_data)
        logger.info(
            f"Local video registered: {file_path.name} (ID={video.id}, {round(file_size/(1024*1024*1024), 2)}GB)"
        )

        # Add annotation count
        response_data = models.VideoResponse.model_validate(video)
        response_data.annotation_count = 0

        return response_data

    except IntegrityError:
        logger.warning("Duplicate local video registration (same filepath)")
        raise HTTPException(status_code=409, detail="Video already registered")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering local video: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# COHORT ENDPOINTS
# ============================================================================

# Organisations this elicitor may lock content to.
#
# The previous query required the user to be a teacher in a course with a
# cohort-sync enrolment instance. No course on this site has ever had one, so
# it returned an empty list for every user including admins — which is why the
# "restrict to my organisation" control was permanently greyed out.
#
# Cohort-sync instances are also the wrong fix: they would enrol every cohort
# member into a course as a side effect, whereas the knowledge silo needs only
# cohort membership (SiloService reads mdl_cohort_members directly).
#
# The source of truth is now the local_orgrequest registry — the same one that
# governs who may approve people into an organisation.
_MANAGED_COHORTS_QUERY = """
    SELECT c.id, c.name
    FROM mdl_local_orgrequest_org o
    JOIN mdl_cohort c ON c.id = o.cohortid
    JOIN mdl_local_orgrequest_manager m ON m.orgid = o.id AND m.userid = %s
    WHERE o.enabled = 1
    ORDER BY c.name
"""

# Site staff can lock content to any published organisation — they cover
# organisations that have not yet nominated their own contact.
_ALL_ORGS_QUERY = """
    SELECT c.id, c.name
    FROM mdl_local_orgrequest_org o
    JOIN mdl_cohort c ON c.id = o.cohortid
    WHERE o.enabled = 1
    ORDER BY c.name
"""


@app.get("/api/cohorts/managed")
async def get_managed_cohorts(
    current_user: MoodleUser = Depends(require_capability("manage")),
):
    """Return cohorts the JWT user is responsible for (has teacher role in an enrolled course).

    Returns empty list if user has no teacher roles — frontend shows the contact-admin message.
    """
    user_id = current_user.userid

    try:
        import pymysql.cursors
        conn = pymysql.connect(
            host="localhost",
            user="moodleuser",
            password=os.getenv("MOODLE_DB_PASSWORD", ""),
            database="moodle",
            cursorclass=pymysql.cursors.DictCursor,
        )
        is_staff = any(role in ('admin', 'manager') for role in (current_user.roles or []))
        with conn:
            with conn.cursor() as cur:
                if is_staff:
                    cur.execute(_ALL_ORGS_QUERY)
                else:
                    cur.execute(_MANAGED_COHORTS_QUERY, (user_id,))
                rows = cur.fetchall()
        return [{"cohort_id": r["id"], "cohort_name": r["name"]} for r in rows]
    except Exception as e:
        logger.error(f"get_managed_cohorts failed: {e}")
        raise HTTPException(status_code=503, detail="Could not query Moodle DB")


# ============================================================================
# PROJECT ENDPOINTS
# ============================================================================


@app.post("/api/projects", response_model=models.ProjectResponse)
async def create_project(
    project: models.ProjectCreate,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(require_capability("manage")),
):
    """Create a new project. Restricted to staff — a project is a shared,
    course-wide structure (not owned by an individual user), so creating
    one is a 'manage' action, not something any logged-in student should do."""
    try:
        if not await moodle_db.cohort_exists(project.allowed_cohort_id):
            raise HTTPException(
                status_code=400,
                detail=f"Unknown cohort id {project.allowed_cohort_id}. Content locked "
                       "to a non-existent cohort would be retrievable by nobody.",
            )
        new_project = await db.create_project(session, project)
        logger.info(f"Project created: {new_project.name} (ID={new_project.id})")
        return new_project
    except Exception as e:
        logger.error(f"Error creating project: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/projects", response_model=List[models.ProjectResponse])
async def get_all_projects(
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Get all projects"""
    try:
        projects = await db.get_all_projects(session)
        return projects
    except Exception as e:
        logger.error(f"Error getting projects: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/projects/{project_id}", response_model=models.ProjectResponse)
async def get_project(
    project_id: int,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Get a specific project by ID"""
    try:
        project = await db.get_project(session, project_id)
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        return project
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting project: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/projects/{project_id}/videos", response_model=List[models.VideoResponse])
async def get_project_videos(
    project_id: int,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Get all videos in a project, ordered by batch position"""
    try:
        project = await db.get_project(session, project_id)
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        videos = await db.get_videos_by_project(session, project_id)

        # Add annotation count to each video
        video_responses = []
        for video in videos:
            video_response = models.VideoResponse.model_validate(video)
            video_response.annotation_count = len(video.annotations)
            video_responses.append(video_response)

        return video_responses
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting project videos: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/projects/{project_id}", response_model=models.ProjectResponse)
async def update_project(
    project_id: int,
    project_update: models.ProjectUpdate,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(require_capability("manage")),
):
    """Update a project. Triggers ChromaDB resync if allowed_cohort_id changes.

    Restricted to staff — reassigning a project's cohort resyncs the shared
    corpus, so this is a 'manage' action."""
    try:
        # Fetch current state before update
        current = await db.get_project(session, project_id)
        if not current:
            raise HTTPException(status_code=404, detail="Project not found")

        if 'allowed_cohort_id' in project_update.model_fields_set:
            if not await moodle_db.cohort_exists(project_update.allowed_cohort_id):
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown cohort id {project_update.allowed_cohort_id}. Content "
                           "locked to a non-existent cohort would be retrievable by nobody.",
                )

        old_cohort_id = current.allowed_cohort_id
        updated_project = await db.update_project(session, project_id, project_update)
        logger.info(f"Project updated: ID={project_id}")

        # If cohort changed, trigger CraftPilot resync asynchronously
        new_cohort_id = updated_project.allowed_cohort_id
        if old_cohort_id != new_cohort_id:
            craftpilot_url = "http://127.0.0.1:8000/api/resync-project-annotations"
            internal_token = os.getenv("INTERNAL_API_TOKEN", "")
            payload = {
                "project_name": updated_project.name,
                "allowed_cohort_id": new_cohort_id,
            }
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    resp = await client.post(
                        craftpilot_url,
                        json=payload,
                        headers={"X-Internal-Token": internal_token},
                    )
                    resp.raise_for_status()
                    logger.info(
                        f"ChromaDB resync triggered for project '{updated_project.name}': "
                        f"cohort {old_cohort_id} → {new_cohort_id}"
                    )
            except Exception as e:
                logger.error(f"ChromaDB resync failed for project {project_id}: {e}")
                # Do not fail the project update — resync can be retried

        return updated_project
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating project: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/projects/{project_id}")
async def delete_project(
    project_id: int,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(require_capability("manage")),
):
    """Delete a project (sets project_id to null for associated videos). Staff-only."""
    try:
        project = await db.get_project(session, project_id)
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        await db.delete_project(session, project_id)
        logger.info(f"Project deleted: ID={project_id}")
        return {"status": "success", "message": "Project deleted"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting project: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Tag API Endpoints


@app.get("/api/tags", response_model=List[models.TagResponse])
async def get_all_tags(
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Get all available tags ordered by usage count"""
    try:
        tags = await db.get_all_tags(session)
        return [models.TagResponse.model_validate(tag) for tag in tags]
    except Exception as e:
        logger.error(f"Error getting tags: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tags/{tag_name}", response_model=models.TagResponse)
async def get_tag(
    tag_name: str,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Get a specific tag by name"""
    try:
        tag = await db.get_tag_by_name(session, tag_name)
        if not tag:
            raise HTTPException(status_code=404, detail="Tag not found")
        return models.TagResponse.model_validate(tag)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting tag: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _authorize_annotation_access(annotation, current_user: MoodleUser, *, manage: bool = False) -> None:
    """Reject cross-user access to an annotation.

    An annotation's owner (the student who recorded it) may always access it.
    Otherwise the caller needs 'viewall' (teachers/admins, read-only actions)
    or 'manage' (editing teachers/admins, mutating actions) per the
    capability map in auth.MoodleUser.has_capability.

    owner_id of 0 means the row predates user_id tracking (moodle_db.py
    defaults missing user_id to 0 on insert) — there's no real owner to
    compare against, so those legacy rows are left accessible rather than
    locked out.
    """
    owner_id = getattr(annotation, "user_id", None)
    if not owner_id or owner_id == current_user.userid:
        return

    required_capability = "manage" if manage else "viewall"
    if not current_user.has_capability(required_capability):
        raise HTTPException(status_code=403, detail="Not authorized to access this annotation")


@app.post("/api/annotations", response_model=models.AnnotationResponse)
async def create_annotation(
    video_id: int,
    start_time: float,
    end_time: float,
    audio_blob: UploadFile = File(...),
    craft: Optional[str] = Form(None),
    task: Optional[str] = Form(None),
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Create a new annotation with audio recording"""
    # 60 annotations per user per hour — each triggers transcription + pipeline.
    _enforce_rate_limit(f"annotate:{current_user.userid}", max_requests=60, window_seconds=3600)
    try:
        # Validate video exists
        video = await db.get_video(session, video_id)
        if not video:
            raise HTTPException(status_code=404, detail="Video not found")

        # Validate times
        if start_time >= end_time:
            raise HTTPException(
                status_code=400, detail="start_time must be less than end_time"
            )

        # Read audio data from uploaded file
        try:
            audio_bytes = await audio_blob.read()
        except Exception as e:
            raise HTTPException(
                status_code=400, detail=f"Failed to read audio data: {str(e)}"
            )

        # Clamp to an allowlist — reject anything that isn't audio so a
        # malicious filename can't store an executable or HTML file on disk.
        _raw_ext = Path(audio_blob.filename or "audio.webm").suffix.lower()
        uploaded_ext = _raw_ext if _raw_ext in _ALLOWED_AUDIO_EXTS else ".webm"
        audio_filename = f"annotation_{uuid.uuid4().hex}{uploaded_ext}"
        audio_filepath = AUDIO_DIR / audio_filename

        # Save audio file
        async with aiofiles.open(audio_filepath, "wb") as f:
            await f.write(audio_bytes)

        logger.info(f"Audio saved: {audio_filename}")

        # Create annotation record
        annotation_data = models.AnnotationCreate(
            video_id=video_id,
            start_time=start_time,
            end_time=end_time,
            audio_filename=audio_filename,
            audio_filepath=str(audio_filepath),
            craft=craft,
            task=task,
            user_id=current_user.userid,
            context_id=current_user.contextid,
        )

        annotation = await db.create_annotation(session, annotation_data)
        logger.info(f"Annotation created: ID={annotation.id}")

        # Broadcast to WebSocket clients
        await manager.broadcast(
            {
                "type": "annotation_created",
                "annotation_id": annotation.id,
                "video_id": video_id,
            }
        )

        # Start transcription in background
        import asyncio

        asyncio.create_task(
            process_transcription(cast(int, annotation.id), str(audio_filepath))
        )

        # Prepare response
        response = models.AnnotationResponse.model_validate(annotation)

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating annotation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Task API Endpoints


@app.get("/api/tasks", response_model=List[models.TaskResponse])
async def list_tasks(
    craft: Optional[str] = None,
    published: Optional[int] = None,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """List tasks with optional craft and published filter."""
    try:
        published_only = None
        if published is not None:
            published_only = True if published == 1 else False
        tasks = await db.get_tasks(session, craft=craft, published_only=published_only)
        return [models.TaskResponse.model_validate(t) for t in tasks]
    except Exception as e:
        logger.error(f"Error listing tasks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/tasks", response_model=models.TaskResponse)
async def create_task(
    task_data: models.TaskCreate,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Create a new task (scoped per craft domain)."""
    try:
        # Check for duplicates within the same craft
        result = await session.execute(
            select(Task).where(
                (Task.name == task_data.name) & (Task.craft == task_data.craft)
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            raise HTTPException(
                status_code=400,
                detail="Task with this name already exists in this craft domain",
            )
        task = await db.create_task(session, task_data)
        return models.TaskResponse.model_validate(task)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/tasks/{task_name}")
async def delete_task(
    task_name: str,
    craft: str,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(require_capability("manage")),
):
    """Delete a task by name and craft. Staff-only — not exposed in the UI,
    and deleting a task definition affects everyone using that craft domain."""
    try:
        deleted = await db.delete_task(session, task_name, craft)
        if not deleted:
            raise HTTPException(status_code=404, detail="Task not found")
        return {
            "status": "success",
            "message": "Task deleted",
            "name": task_name,
            "craft": craft,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting task: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/crafts", response_model=List[models.CustomCraftResponse])
async def list_custom_crafts(
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Return the authenticated user's custom craft domains."""
    try:
        crafts = await db.get_custom_crafts_by_user(str(current_user.userid))
        return [models.CustomCraftResponse(**c) for c in crafts]
    except Exception as e:
        logger.error(f"Error listing custom crafts: {e}")
        raise HTTPException(status_code=500, detail="Could not load custom crafts")


@app.post("/api/crafts", response_model=models.CustomCraftResponse, status_code=201)
async def create_custom_craft_endpoint(
    payload: models.CustomCraftCreate,
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Create a new personal craft domain for the authenticated user.

    Security: user_id is taken exclusively from the verified JWT — never from
    the request body — so one user cannot create crafts on behalf of another.
    """
    try:
        craft = await db.create_custom_craft(str(current_user.userid), payload.craft_label)
        return models.CustomCraftResponse(**craft)
    except IntegrityError:
        raise HTTPException(status_code=409, detail="This craft domain already exists")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating custom craft: {e}")
        raise HTTPException(status_code=500, detail="Could not save custom craft")


async def push_annotation_to_rag(annotation_id: int, transcription: str) -> None:
    """Fire-and-forget: push a completed annotation to the CraftPilot RAG backend.

    Called after transcription_status is set to 'completed' so that every new
    elicitation is immediately searchable without waiting for a manual sync.
    Errors are logged but never propagate — the transcription pipeline must not
    be affected by RAG availability.
    """
    import urllib.request
    import urllib.error

    RAG_INGEST_URL = "http://127.0.0.1:8000/api/ingest-annotation"

    try:
        async with db.AsyncSessionLocal() as session:
            annotation = await db.get_annotation(session, annotation_id)
            if not annotation:
                logger.warning(f"push_annotation_to_rag: annotation {annotation_id} not found")
                return

            video_id = annotation.get("video_id") or annotation.get("videoid")
            video = await db.get_video(session, video_id) if video_id else None
            project_name = "unknown"
            project = None
            if video and video.get("project_id"):
                project = await db.get_project(session, video["project_id"])
                if project:
                    project_name = project.get("name", "unknown")

        payload = json.dumps({
            "annotation_id":    annotation_id,
            "video_id":         video_id or 0,
            "transcription":    transcription,
            "start_time":       float(annotation.get("start_time") or annotation.get("starttime") or 0),
            "end_time":         float(annotation.get("end_time")   or annotation.get("endtime")   or 0),
            "video_filename":   (video or {}).get("filename", "unknown.mp4"),
            "video_filepath":   (video or {}).get("filepath", ""),
            "source_type":      (video or {}).get("source_type", "local"),
            "project_name":     project_name,
            "audio_filepath":   annotation.get("audio_filepath") or annotation.get("audiofilepath") or "",
            "language":         annotation.get("language"),
            "allowed_cohort_id": project.get("allowed_cohort_id") if project else None,
            # craft drives CraftPilot's CRAFT_COHORT_MAP safety net, which keeps
            # partner content restricted when a project has no cohort set.
            "craft":            annotation.get("craft"),
            "task":             annotation.get("task"),
            "annotation_created_at": str(annotation.get("timecreated") or annotation.get("annotation_created_at") or ""),
            "annotation_updated_at": str(annotation.get("timemodified") or annotation.get("annotation_updated_at") or ""),
        }).encode()

        internal_token = os.getenv("INTERNAL_API_TOKEN", "")

        def _post():
            req = urllib.request.Request(
                RAG_INGEST_URL,
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Internal-Token": internal_token,
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status

        import asyncio
        loop = asyncio.get_running_loop()
        status = await loop.run_in_executor(None, _post)
        logger.info(f"Annotation {annotation_id} pushed to RAG backend (HTTP {status})")

    except urllib.error.URLError as e:
        logger.warning(f"RAG backend unreachable for annotation {annotation_id}: {e.reason}")
    except Exception as e:
        logger.error(f"push_annotation_to_rag failed for annotation {annotation_id}: {e}")


async def process_transcription(annotation_id: int, audio_path: str):
    """Background task to process transcription"""
    try:
        # Update status to processing
        async with db.AsyncSessionLocal() as session:
            await db.update_annotation(
                session,
                annotation_id,
                models.AnnotationUpdate(transcription_status="processing"),
            )

        # Broadcast status
        await manager.broadcast(
            {
                "type": "transcription_status",
                "annotation_id": annotation_id,
                "status": "processing",
            }
        )

        logger.info(f"Starting transcription for annotation {annotation_id}")

        # Perform transcription
        stt_result = await transcribe_audio(audio_path)
        transcription = stt_result["text"]
        language = stt_result.get("language")
        if language == "unknown":
            language = None

        # Guard: treat empty transcription as a failure so the pipeline doesn't run
        if not transcription or not transcription.strip():
            logger.warning(f"Empty transcription for annotation {annotation_id} — microphone may be muted")
            async with db.AsyncSessionLocal() as session:
                await db.update_annotation(
                    session,
                    annotation_id,
                    models.AnnotationUpdate(transcription_status="failed"),
                )
            await manager.broadcast(
                {
                    "type": "transcription_error",
                    "annotation_id": annotation_id,
                    "error": "Transcription vide — vérifiez que le microphone n'est pas coupé.",
                }
            )
            return

        # Update annotation with transcription
        async with db.AsyncSessionLocal() as session:
            await db.update_annotation(
                session,
                annotation_id,
                models.AnnotationUpdate(
                    transcription=transcription,
                    transcription_status="completed",
                    language=language,
                ),
            )

        logger.info(f"Transcription completed for annotation {annotation_id} (language={language})")

        # Broadcast completion
        await manager.broadcast(
            {
                "type": "transcription_complete",
                "annotation_id": annotation_id,
                "transcription": transcription,
                "language": language,
            }
        )

        # Start Judge to determine if review is needed
        import asyncio

        asyncio.create_task(process_judge(annotation_id, transcription, None, language))

        # Push to CraftPilot RAG backend so the elicitation is immediately searchable
        asyncio.create_task(push_annotation_to_rag(annotation_id, transcription))

    except Exception as e:
        logger.error(f"Transcription error for annotation {annotation_id}: {e}")

        # Update status to failed
        try:
            async with db.AsyncSessionLocal() as session:
                await db.update_annotation(
                    session,
                    annotation_id,
                    models.AnnotationUpdate(transcription_status="failed"),
                )

            await manager.broadcast(
                {
                    "type": "transcription_error",
                    "annotation_id": annotation_id,
                    "error": str(e),
                }
            )
        except:
            pass


@app.post("/api/annotations/transcribe-only")
async def transcribe_only(
    audio_blob: UploadFile = File(...),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """
    Lightweight transcription endpoint for the guided Q&A voice enrichment flow.
    Accepts a raw audio blob, transcribes it, and returns the text.
    Does NOT create any annotation record or trigger any pipeline.
    """
    # 20 transcriptions per user per hour — transcription is CPU/API-intensive.
    _enforce_rate_limit(f"transcribe:{current_user.userid}", max_requests=20, window_seconds=3600)
    import tempfile

    tmp_path = None
    try:
        _raw_suffix = Path(audio_blob.filename or "audio.webm").suffix.lower()
        suffix = _raw_suffix if _raw_suffix in _ALLOWED_AUDIO_EXTS else ".webm"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp_path = tmp.name
            content = await audio_blob.read()
            tmp.write(content)

        stt_result = await transcribe_audio(tmp_path)
        transcription = stt_result["text"]

        if not transcription or not transcription.strip():
            raise HTTPException(status_code=400, detail="Empty transcription — check microphone")

        return {"transcription": transcription.strip()}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"transcribe-only error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


@app.post("/api/annotations/{annotation_id}/rerecord")
async def rerecord_annotation(
    annotation_id: int,
    audio_blob: UploadFile = File(...),
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Replace an annotation's audio with a freshly recorded take.

    Re-running Whisper on unchanged audio always yields the same
    transcription, so this endpoint instead lets the user re-voice the
    segment: the new recording replaces the stored audio file, and the
    transcription is recomputed from it (overwriting, not appending to,
    the previous transcription).
    """
    _enforce_rate_limit(f"rerecord:{current_user.userid}", max_requests=30, window_seconds=3600)

    annotation = await db.get_annotation(session, annotation_id)
    if not annotation:
        raise HTTPException(status_code=404, detail="Annotation not found")
    _authorize_annotation_access(annotation, current_user, manage=True)

    try:
        audio_bytes = await audio_blob.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read audio data: {str(e)}")

    _raw_ext = Path(audio_blob.filename or "audio.webm").suffix.lower()
    uploaded_ext = _raw_ext if _raw_ext in _ALLOWED_AUDIO_EXTS else ".webm"
    new_filename = f"annotation_{uuid.uuid4().hex}{uploaded_ext}"
    new_audio_path = AUDIO_DIR / new_filename

    async with aiofiles.open(new_audio_path, "wb") as f:
        await f.write(audio_bytes)

    old_audio_path = str(annotation.audio_filepath or "")

    # Point the annotation at the new audio and clear downstream fields so
    # the UI drops stale transcription/coverage/tag/review data.
    # NOTE: audio_filepath is deliberately NOT a field on models.AnnotationUpdate
    # (that schema is accepted as a raw request body by PUT /api/annotations/{id},
    # so exposing a filesystem path there would let a client point an
    # annotation at an arbitrary path — which the deletion below would then
    # remove from disk). Passed as a plain dict instead, since db.update_annotation
    # forwards non-pydantic values through to moodle_db unchanged.
    await db.update_annotation(
        session,
        annotation_id,
        {
            "audio_filepath": str(new_audio_path),
            "transcription_status": "processing",
            "transcription": None,
            "language": None,
        },
    )

    # Defense in depth: only ever delete a file that resolves inside AUDIO_DIR,
    # even though audio_filepath is no longer client-settable.
    if old_audio_path and old_audio_path != str(new_audio_path):
        try:
            resolved_old = Path(old_audio_path).resolve()
            if resolved_old.is_relative_to(AUDIO_DIR.resolve()) and resolved_old.exists():
                os.remove(resolved_old)
        except Exception as e:
            logger.warning(f"Failed to remove old audio file {old_audio_path}: {e}")

    import asyncio
    asyncio.create_task(process_transcription(annotation_id, str(new_audio_path)))
    return {"status": "queued", "annotation_id": annotation_id}


@app.post("/api/annotations/{annotation_id}/regenerate-extended")
async def regenerate_extended_transcript(
    annotation_id: int,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Acknowledge a request to regenerate the extended transcript.

    Extended transcripts are not yet supported in the Moodle-backed version of
    this tool, so this endpoint returns immediately with a no-op response and
    broadcasts an extended_transcript_complete event so the frontend clears its
    "Regenerating…" state without showing an error.
    """
    annotation = await db.get_annotation(session, annotation_id)
    if not annotation:
        raise HTTPException(status_code=404, detail="Annotation not found")
    _authorize_annotation_access(annotation, current_user, manage=True)

    await manager.broadcast({
        "type": "extended_transcript_complete",
        "annotation_id": annotation_id,
        "extended_transcript": None,
    })
    return {"status": "ok", "annotation_id": annotation_id}


@app.post("/api/annotations/{annotation_id}/review")
async def review_annotation(
    annotation_id: int,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """
    Trigger AI review of an elicitation to identify gaps in HOW/EVALUATION/FEEDBACK dimensions.
    Returns review results with prompts for missing information.
    """
    # 30 LLM reviews per user per hour.
    _enforce_rate_limit(f"review:{current_user.userid}", max_requests=30, window_seconds=3600)
    try:
        # Verify annotation exists and has transcription
        annotation = await db.get_annotation(session, annotation_id)
        if not annotation:
            raise HTTPException(status_code=404, detail="Annotation not found")
        _authorize_annotation_access(annotation, current_user, manage=True)

        transcription_text = (
            str(annotation.transcription)
            if annotation.transcription is not None
            else None
        )
        if not transcription_text:
            raise HTTPException(
                status_code=400,
                detail="Cannot review annotation: no transcription available",
            )

        logger.info(f"Triggering AI review for annotation {annotation_id}")

        # Prepare update data - increment review attempts
        attempts_val = (
            annotation.review_attempts if annotation.review_attempts is not None else 0
        )
        update_data = models.AnnotationUpdate(
            review_status="processing",
            review_attempts=(
                int(attempts_val) + 1 if isinstance(attempts_val, int) else 1
            ),
        )

        # Update review status to processing
        await db.update_annotation(session, annotation_id, update_data)

        # Broadcast status update
        await manager.broadcast(
            {
                "type": "review_status",
                "annotation_id": annotation_id,
                "status": "processing",
            }
        )

        # Start review in background
        import asyncio

        craft_str = str(annotation.craft) if annotation.craft is not None else None
        tags_payload = None
        tags_str = cast(Optional[str], annotation.tags)
        if tags_str:
            try:
                tags_payload = json.loads(tags_str)
            except Exception:
                tags_payload = None
        asyncio.create_task(
            process_review(annotation_id, transcription_text, craft_str, tags_payload)
        )

        return {
            "status": "success",
            "message": "AI review started",
            "annotation_id": annotation_id,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering AI review: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/annotations/{annotation_id}/tags")
async def tag_annotation(
    annotation_id: int,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """
    Trigger AI tagging to extract metadata from an elicitation transcription.
    Returns extracted tags and status updates via WebSocket.
    Increments tagging_trigger_number to track how many times tagging has been triggered.
    """
    try:
        logger.error(f"[TAGGING] Tag relaunch requested for annotation {annotation_id}")
        print(f"[TAGGING] Tag relaunch requested for annotation {annotation_id}")
        # Verify annotation exists and has transcription
        annotation = await db.get_annotation(session, annotation_id)
        if not annotation:
            raise HTTPException(status_code=404, detail="Annotation not found")
        _authorize_annotation_access(annotation, current_user, manage=True)

        transcription_text = (
            str(annotation.transcription)
            if annotation.transcription is not None
            else None
        )
        if not transcription_text:
            raise HTTPException(
                status_code=400,
                detail="Cannot tag annotation: no transcription available",
            )

        logger.error(
            f"[TAGGING] Transcription present for annotation {annotation_id} (length={len(transcription_text)} chars)"
        )
        print(
            f"[TAGGING] Transcription present for annotation {annotation_id} (length={len(transcription_text)} chars)"
        )

        logger.error(
            f"[TAGGING] Triggering tag extraction for annotation {annotation_id}"
        )
        print(f"[TAGGING] Triggering tag extraction for annotation {annotation_id}")

        # Increment tagging trigger count
        current_count = (
            annotation.tagging_trigger_number
            if annotation.tagging_trigger_number is not None
            else 0
        )
        new_count = current_count + 1

        # Update tagging status and trigger count
        logger.error(
            f"[TAGGING] Updating status=processing and trigger_count={new_count} for annotation {annotation_id}"
        )
        print(
            f"[TAGGING] Updating status=processing and trigger_count={new_count} for annotation {annotation_id}"
        )
        await db.update_annotation(
            session,
            annotation_id,
            models.AnnotationUpdate(
                tagging_status="processing", tagging_trigger_number=new_count
            ),
        )

        # Broadcast status update
        logger.error(
            f"[TAGGING] Broadcasting tagging_status=processing for annotation {annotation_id}"
        )
        print(
            f"[TAGGING] Broadcasting tagging_status=processing for annotation {annotation_id}"
        )
        await manager.broadcast(
            {
                "type": "tagging_status",
                "annotation_id": annotation_id,
                "status": "processing",
                "trigger_count": new_count,
            }
        )
        await manager.broadcast(
            {
                "type": "tagging_debug",
                "annotation_id": annotation_id,
                "message": "Tagging task scheduled",
            }
        )

        # Start tagging in background
        import asyncio

        craft_str = str(annotation.craft) if annotation.craft is not None else None
        language_str = str(annotation.language) if annotation.language is not None else None
        logger.error(
            f"[TAGGING] Starting background task for annotation {annotation_id} (craft={craft_str})"
        )
        print(
            f"[TAGGING] Starting background task for annotation {annotation_id} (craft={craft_str})"
        )
        asyncio.create_task(
            process_tagging(annotation_id, transcription_text, craft_str, language_str)
        )

        return {
            "status": "success",
            "message": "Tag extraction started",
            "annotation_id": annotation_id,
            "trigger_count": new_count,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error triggering tags for annotation {annotation_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/diagnostics/tagging-llm")
async def tagging_llm_diagnostics(
    current_user: MoodleUser = Depends(require_capability("manage")),
):
    """Return last LLM tagging request diagnostics. Staff-only — this can
    surface internal error text from the LLM pipeline."""
    import tagging_service

    return {
        "last_request_at": tagging_service.LAST_LLM_TAG_REQUEST_AT,
        "last_status": tagging_service.LAST_LLM_TAG_STATUS,
        "last_error": tagging_service.LAST_LLM_TAG_ERROR,
    }


async def process_judge(
    annotation_id: int,
    transcription: str,
    craft: Optional[str] = None,
    language: Optional[str] = None,
):
    """Background task to judge if AI review is needed using judge_service"""
    from judge_service import judge_elicitation

    try:
        logger.info(f"Starting judge evaluation for annotation {annotation_id}")

        # Update status to processing
        async with db.AsyncSessionLocal() as session:
            await db.update_annotation(
                session,
                annotation_id,
                models.AnnotationUpdate(judge_status="processing"),
            )

        # Broadcast status
        await manager.broadcast(
            {
                "type": "judge_status",
                "annotation_id": annotation_id,
                "status": "processing",
            }
        )

        # Judge whether review is needed
        judge_result = await judge_elicitation(transcription, craft, language)

        # Store judge decision
        async with db.AsyncSessionLocal() as session:
            await db.update_annotation(
                session,
                annotation_id,
                models.AnnotationUpdate(
                    judge_status="completed",
                    judge_decision=json.dumps(judge_result, ensure_ascii=False),
                ),
            )

        logger.info(
            f"Judge completed for annotation {annotation_id}: needs_review={judge_result.get('needs_review')}"
        )

        # Broadcast judge decision
        await manager.broadcast(
            {
                "type": "judge_complete",
                "annotation_id": annotation_id,
                "judge_decision": judge_result,
            }
        )

        # If judge says review is needed, auto-trigger tagging/review flow
        if judge_result.get("needs_review", True):
            logger.info(
                f"Judge recommends review for annotation {annotation_id} - auto-triggering"
            )
            import asyncio

            asyncio.create_task(process_tagging(annotation_id, transcription, craft, language))
        else:
            logger.info(
                f"Judge says annotation {annotation_id} is complete - no review needed (manual trigger available)"
            )

    except Exception as e:
        logger.error(f"Judge error for annotation {annotation_id}: {e}")

        # Update status to failed
        async with db.AsyncSessionLocal() as session:
            await db.update_annotation(
                session,
                annotation_id,
                models.AnnotationUpdate(judge_status="failed"),
            )

        # Broadcast error (but fall through to auto-trigger tagging to be safe)
        await manager.broadcast(
            {
                "type": "judge_error",
                "annotation_id": annotation_id,
                "error": str(e),
            }
        )

        # On judge error, still trigger tagging to be safe
        import asyncio

        asyncio.create_task(process_tagging(annotation_id, transcription, craft, language))


async def process_task_detection(
    annotation_id: int,
    transcription: str,
    craft: Optional[str] = None,
    language: Optional[str] = None,
):
    """Background task to detect the main task from elicitation transcription"""
    from task_detector_service import detect_task
    import asyncio

    try:
        logger.error(
            f"[TASK_DETECTION] Starting task detection for annotation {annotation_id}"
        )
        print(
            f"[TASK_DETECTION] Starting task detection for annotation {annotation_id}"
        )

        # Update status to processing
        async with db.AsyncSessionLocal() as session:
            await db.update_annotation(
                session,
                annotation_id,
                models.AnnotationUpdate(detected_task_status="processing"),
            )

        # Broadcast processing status
        await manager.broadcast(
            {
                "type": "task_detection_status",
                "annotation_id": annotation_id,
                "status": "processing",
            }
        )

        # Detect task using LLM with timeout
        logger.error(
            f"[TASK_DETECTION] Calling detect_task for annotation {annotation_id}"
        )
        print(f"[TASK_DETECTION] Calling detect_task for annotation {annotation_id}")
        try:
            task_result = await asyncio.wait_for(
                detect_task(transcription, craft or "jewelry", language), timeout=60
            )
        except asyncio.TimeoutError:
            raise Exception("Task detection timed out after 60 seconds")

        detected_task = task_result.get("detected_task")
        confidence = task_result.get("confidence", 0.0)
        reasoning = task_result.get("reasoning", "")

        logger.error(
            f"[TASK_DETECTION] Task detection result: task={detected_task}, confidence={confidence}"
        )
        print(
            f"[TASK_DETECTION] Task detection result: task={detected_task}, confidence={confidence}"
        )

        # Only store if detected_task is not null and confidence >= 0.5
        if detected_task and confidence >= 0.5:
            logger.error(
                f"[TASK_DETECTION] Storing detected task '{detected_task}' for annotation {annotation_id}"
            )
            print(
                f"[TASK_DETECTION] Storing detected task '{detected_task}' for annotation {annotation_id}"
            )
            async with db.AsyncSessionLocal() as session:
                await db.update_annotation(
                    session,
                    annotation_id,
                    models.AnnotationUpdate(
                        detected_task=detected_task,
                        detected_task_confidence=confidence,
                        detected_task_status="completed",
                    ),
                )
        else:
            logger.error(
                f"[TASK_DETECTION] Task detection skipped (task=null or low confidence) for annotation {annotation_id}"
            )
            print(
                f"[TASK_DETECTION] Task detection skipped (task=null or low confidence) for annotation {annotation_id}"
            )
            async with db.AsyncSessionLocal() as session:
                await db.update_annotation(
                    session,
                    annotation_id,
                    models.AnnotationUpdate(detected_task_status="completed"),
                )

        # Broadcast completion
        logger.error(
            f"[TASK_DETECTION] Broadcasting task_detection_complete for annotation {annotation_id}"
        )
        print(
            f"[TASK_DETECTION] Broadcasting task_detection_complete for annotation {annotation_id}"
        )
        await manager.broadcast(
            {
                "type": "task_detection_complete",
                "annotation_id": annotation_id,
                "detected_task": detected_task,
                "confidence": confidence,
                "reasoning": reasoning,
            }
        )

        # Trigger review after task detection
        logger.error(
            f"[TASK_DETECTION] Triggering review for annotation {annotation_id}"
        )
        print(f"[TASK_DETECTION] Triggering review for annotation {annotation_id}")

        # Get the current annotation to pass tags to review
        async with db.AsyncSessionLocal() as session:
            annotation = await db.get_annotation(session, annotation_id)
            tags_list = []
            if annotation is not None and annotation.tags is not None:
                try:
                    tags_list = json.loads(annotation.tags)
                except (json.JSONDecodeError, TypeError):
                    tags_list = []

        await process_review(
            annotation_id,
            transcription,
            craft,
            tags_list,
            trigger_tagging=False,
        )

    except Exception as e:
        logger.error(
            f"[TASK_DETECTION ERROR] Task detection error for annotation {annotation_id}: {e}",
            exc_info=True,
        )

        # Update status to failed
        async with db.AsyncSessionLocal() as session:
            await db.update_annotation(
                session,
                annotation_id,
                models.AnnotationUpdate(detected_task_status="failed"),
            )

        # Broadcast error
        await manager.broadcast(
            {
                "type": "task_detection_error",
                "annotation_id": annotation_id,
                "error": str(e),
            }
        )

        # Still trigger review to continue the pipeline
        logger.error(
            f"[TASK_DETECTION] Triggering review after error for annotation {annotation_id}"
        )
        try:
            async with db.AsyncSessionLocal() as session:
                annotation = await db.get_annotation(session, annotation_id)
                tags_list = []
                if annotation is not None and annotation.tags is not None:
                    try:
                        tags_list = json.loads(annotation.tags)
                    except (json.JSONDecodeError, TypeError):
                        tags_list = []

            await process_review(
                annotation_id,
                transcription,
                craft,
                tags_list,
                trigger_tagging=False,
            )
        except Exception as review_error:
            logger.error(
                f"[TASK_DETECTION] Failed to trigger review after task detection error: {review_error}",
                exc_info=True,
            )


async def process_review(
    annotation_id: int,
    transcription: str,
    craft: Optional[str] = None,
    tags: Optional[List[Dict[str, str]]] = None,
    trigger_tagging: bool = True,
):
    """No-op — replaced by spaCy coverage detector + Infomaniak summary (2026-04-21)."""
    pass


async def process_tagging(
    annotation_id: int,
    transcription: str,
    craft: Optional[str] = None,
    language: Optional[str] = None,
):
    """Background task to extract and apply tags using tagging_service"""
    from tagging_service import extract_tags
    import asyncio

    try:
        logger.error(
            f"[TAGGING] Starting tag extraction for annotation {annotation_id}"
        )
        print(f"[TAGGING] Starting tag extraction for annotation {annotation_id}")
        await manager.broadcast(
            {
                "type": "tagging_debug",
                "annotation_id": annotation_id,
                "message": "Tagging task started",
            }
        )
        logger.debug(
            f"[TAGGING] Transcription length: {len(transcription)} chars, Craft: {craft}"
        )

        # Update status to processing
        async with db.AsyncSessionLocal() as session:
            logger.error(
                f"[TAGGING] Set tagging_status=processing for annotation {annotation_id}"
            )
            print(
                f"[TAGGING] Set tagging_status=processing for annotation {annotation_id}"
            )
            await db.update_annotation(
                session,
                annotation_id,
                models.AnnotationUpdate(tagging_status="processing"),
            )

        # Broadcast status
        logger.error(
            f"[TAGGING] Broadcast tagging_status=processing for annotation {annotation_id}"
        )
        print(
            f"[TAGGING] Broadcast tagging_status=processing for annotation {annotation_id}"
        )
        await manager.broadcast(
            {
                "type": "tagging_status",
                "annotation_id": annotation_id,
                "status": "processing",
            }
        )
        await manager.broadcast(
            {
                "type": "tagging_debug",
                "annotation_id": annotation_id,
                "message": "tagging_status=processing broadcasted",
            }
        )

        # Extract tags using LLM with timeout
        logger.error(f"[TAGGING] Calling extract_tags for annotation {annotation_id}")
        print(f"[TAGGING] Calling extract_tags for annotation {annotation_id}")
        await manager.broadcast(
            {
                "type": "tagging_debug",
                "annotation_id": annotation_id,
                "message": "Calling extract_tags (LLM)",
            }
        )
        try:
            tags_result = await asyncio.wait_for(
                extract_tags(transcription, craft), timeout=90
            )
        except asyncio.TimeoutError:
            raise Exception("Tag extraction timed out after 90 seconds")
        logger.error(f"[TAGGING] extract_tags returned for annotation {annotation_id}")
        print(f"[TAGGING] extract_tags returned for annotation {annotation_id}")
        await manager.broadcast(
            {
                "type": "tagging_debug",
                "annotation_id": annotation_id,
                "message": "extract_tags completed",
            }
        )

        # Store tags in annotation and update tag registry
        tags_list = tags_result.get("tags", [])
        logger.error(
            f"[TAGGING] Storing {len(tags_list)} tags for annotation {annotation_id}"
        )
        print(f"[TAGGING] Storing {len(tags_list)} tags for annotation {annotation_id}")
        await manager.broadcast(
            {
                "type": "tagging_debug",
                "annotation_id": annotation_id,
                "message": f"Storing {len(tags_list)} tags",
            }
        )
        async with db.AsyncSessionLocal() as session:
            for tag_info in tags_list:
                tag_name = tag_info.get("name")
                tag_category = tag_info.get("category")
                if tag_name and tag_category:
                    await db.get_or_create_tag(session, tag_name, tag_category)
                    await db.increment_tag_usage(session, tag_name)

            await db.update_annotation(
                session,
                annotation_id,
                models.AnnotationUpdate(
                    tags=json.dumps(tags_list, ensure_ascii=False),
                    tagging_status="completed",
                ),
            )

        logger.error(
            f"[TAGGING] Tag extraction completed for annotation {annotation_id}: {len(tags_list)} tags"
        )
        print(
            f"[TAGGING] Tag extraction completed for annotation {annotation_id}: {len(tags_list)} tags"
        )
        await manager.broadcast(
            {
                "type": "tagging_debug",
                "annotation_id": annotation_id,
                "message": f"Tagging completed with {len(tags_list)} tags",
            }
        )

        # Broadcast completion
        logger.error(
            f"[TAGGING] Broadcasting tagging_complete for annotation {annotation_id}"
        )
        print(f"[TAGGING] Broadcasting tagging_complete for annotation {annotation_id}")
        await manager.broadcast(
            {
                "type": "tagging_complete",
                "annotation_id": annotation_id,
                "tags": tags_list,
            }
        )

        # Update review status to processing (tag-aware review)
        logger.error(
            f"[TAGGING] Updating review_status=processing for annotation {annotation_id}"
        )
        print(
            f"[TAGGING] Updating review_status=processing for annotation {annotation_id}"
        )
        async with db.AsyncSessionLocal() as session:
            annotation = await db.get_annotation(session, annotation_id)
            attempts_val = (
                annotation.review_attempts
                if annotation and annotation.review_attempts is not None
                else 0
            )
            await db.update_annotation(
                session,
                annotation_id,
                models.AnnotationUpdate(
                    review_status="processing",
                    review_attempts=(
                        int(attempts_val) + 1 if isinstance(attempts_val, int) else 1
                    ),
                ),
            )

        await manager.broadcast(
            {
                "type": "review_status",
                "annotation_id": annotation_id,
                "status": "processing",
            }
        )

        # Trigger task detection (which will trigger review after)
        logger.error(
            f"[TAGGING] Triggering task detection for annotation {annotation_id}"
        )
        print(f"[TAGGING] Triggering task detection for annotation {annotation_id}")
        await process_task_detection(
            annotation_id,
            transcription,
            craft,
            language,
        )

    except Exception as e:
        logger.error(
            f"[TAGGING ERROR] Tag extraction error for annotation {annotation_id}: {e}",
            exc_info=True,
        )
        await manager.broadcast(
            {
                "type": "tagging_debug",
                "annotation_id": annotation_id,
                "message": f"Tagging error: {e}",
            }
        )

        # Update status to failed
        async with db.AsyncSessionLocal() as session:
            await db.update_annotation(
                session,
                annotation_id,
                models.AnnotationUpdate(tagging_status="failed"),
            )

        # Broadcast error
        await manager.broadcast(
            {
                "type": "tagging_error",
                "annotation_id": annotation_id,
                "error": str(e),
            }
        )

        # Update review status to processing (fallback review)
        async with db.AsyncSessionLocal() as session:
            annotation = await db.get_annotation(session, annotation_id)
            attempts_val = (
                annotation.review_attempts
                if annotation and annotation.review_attempts is not None
                else 0
            )
            await db.update_annotation(
                session,
                annotation_id,
                models.AnnotationUpdate(
                    review_status="processing",
                    review_attempts=(
                        int(attempts_val) + 1 if isinstance(attempts_val, int) else 1
                    ),
                ),
            )

        await manager.broadcast(
            {
                "type": "review_status",
                "annotation_id": annotation_id,
                "status": "processing",
            }
        )

        # Fallback: trigger review without tags (skip auto-tagging to avoid loops)
        await process_review(
            annotation_id,
            transcription,
            craft,
            None,
            trigger_tagging=False,
        )




@app.get("/api/annotations", response_model=List[models.AnnotationResponse])
async def list_annotations(
    video_id: Optional[int] = None,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Get annotations, optionally filtered by video_id"""
    try:
        if video_id:
            video = await db.get_video(session, video_id)
            if not video:
                raise HTTPException(status_code=404, detail="Video not found")
            video_owner_id = getattr(video, "user_id", None)
            if (
                video_owner_id
                and video_owner_id != current_user.userid
                and not current_user.has_capability("viewall")
            ):
                raise HTTPException(status_code=403, detail="Not authorized to access this video's annotations")
            annotations = await db.get_annotations_by_video(session, video_id)
        else:
            # Get all annotations (not typically used)
            annotations = []

        response_annotations = [
            models.AnnotationResponse.model_validate(ann) for ann in annotations
        ]

        return response_annotations

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing annotations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/annotations/{annotation_id}", response_model=models.AnnotationResponse)
async def get_annotation(
    annotation_id: int,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Get annotation by ID"""
    try:
        annotation = await db.get_annotation(session, annotation_id)
        if not annotation:
            raise HTTPException(status_code=404, detail="Annotation not found")
        _authorize_annotation_access(annotation, current_user)

        return models.AnnotationResponse.model_validate(annotation)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting annotation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/annotations/{annotation_id}", response_model=models.AnnotationResponse)
async def update_annotation(
    annotation_id: int,
    update_data: models.AnnotationUpdate,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Update annotation (typically transcription)"""
    try:
        existing = await db.get_annotation(session, annotation_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Annotation not found")
        _authorize_annotation_access(existing, current_user, manage=True)

        annotation = await db.update_annotation(session, annotation_id, update_data)
        if not annotation:
            raise HTTPException(status_code=404, detail="Annotation not found")

        return models.AnnotationResponse.model_validate(annotation)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating annotation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/annotations/{annotation_id}")
async def delete_annotation(
    annotation_id: int,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Delete an annotation"""
    try:
        annotation = await db.get_annotation(session, annotation_id)
        if not annotation:
            raise HTTPException(status_code=404, detail="Annotation not found")
        _authorize_annotation_access(annotation, current_user, manage=True)

        # Delete audio file
        if os.path.exists(str(annotation.audio_filepath)):
            os.remove(str(annotation.audio_filepath))

        # Delete from database
        await db.delete_annotation(session, annotation_id)

        logger.info(f"Annotation deleted: ID={annotation_id}")

        # Broadcast deletion
        await manager.broadcast(
            {"type": "annotation_deleted", "annotation_id": annotation_id}
        )

        return {"status": "success", "message": "Annotation deleted"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting annotation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/annotations/{annotation_id}/feedback")
async def submit_feedback(
    annotation_id: int,
    feedback_data: models.FeedbackRequest,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Submit user feedback for annotation quality"""
    try:
        # Verify annotation exists
        annotation = await db.get_annotation(session, annotation_id)
        if not annotation:
            raise HTTPException(status_code=404, detail="Annotation not found")
        _authorize_annotation_access(annotation, current_user, manage=True)

        # Convert feedback_choices array to JSON string
        feedback_choices_json = json.dumps(feedback_data.feedback_choices)

        # Update annotation with feedback
        await db.update_annotation(
            session,
            annotation_id,
            models.AnnotationUpdate(
                feedback=feedback_data.feedback, feedback_choices=feedback_choices_json
            ),
        )

        logger.info(
            f"Feedback submitted for annotation {annotation_id}: {'positive' if feedback_data.feedback == 1 else 'negative'}"
        )

        return {
            "status": "success",
            "message": "Feedback submitted successfully",
            "annotation_id": annotation_id,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error submitting feedback: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/maintenance/auto-trigger-tagging")
async def auto_trigger_tagging_maintenance(
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(require_capability("manage")),
):
    """
    Maintenance endpoint that auto-triggers tagging for all annotations
    where tagging_trigger_number is 0 (i.e., never been tagged yet).
    Returns count of annotations triggered.

    Staff-only and rate-limited: this fires one paid LLM call per untagged
    annotation in the whole database, so it must not be callable by anyone
    without a login, and not repeatedly in a tight loop.
    """
    _enforce_rate_limit(
        f"auto_tag_maintenance:{current_user.userid}", max_requests=5, window_seconds=3600
    )
    try:
        logger.info(
            "Starting maintenance: auto-triggering tagging for annotations with trigger_count=0"
        )

        # Get all annotations with transcription and trigger_count = 0
        from sqlalchemy import select
        from sqlalchemy.sql import func

        query = select(models.Annotation).where(
            (models.Annotation.tagging_trigger_number == 0)
            | (models.Annotation.tagging_trigger_number.is_(None))
        )
        result = await session.execute(query)
        annotations_to_tag = result.scalars().all()

        triggered_count = 0
        for annotation in annotations_to_tag:
            if not annotation.transcription:
                logger.debug(f"Skipping annotation {annotation.id}: no transcription")
                continue

            logger.info(f"Auto-triggering tagging for annotation {annotation.id}")
            craft_str = str(annotation.craft) if annotation.craft else None
            language_str = getattr(annotation, "language", None)

            # Use the same tagging trigger as the API endpoint
            current_count = (
                annotation.tagging_trigger_number
                if annotation.tagging_trigger_number is not None
                else 0
            )
            new_count = current_count + 1

            await db.update_annotation(
                session,
                annotation.id,
                models.AnnotationUpdate(
                    tagging_status="processing", tagging_trigger_number=new_count
                ),
            )

            # Broadcast status
            await manager.broadcast(
                {
                    "type": "tagging_status",
                    "annotation_id": annotation.id,
                    "status": "processing",
                    "trigger_count": new_count,
                }
            )

            # Trigger background tagging task
            import asyncio

            asyncio.create_task(
                process_tagging(annotation.id, annotation.transcription, craft_str, language_str)
            )
            triggered_count += 1

        logger.info(
            f"Maintenance completed: triggered tagging for {triggered_count} annotations"
        )

        return {
            "status": "success",
            "message": f"Auto-triggered tagging for {triggered_count} annotations",
            "triggered_count": triggered_count,
        }

    except Exception as e:
        logger.error(f"Error in auto-trigger-tagging maintenance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/maintenance/deduplicate-tags")
async def deduplicate_tags_maintenance(
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(require_capability("manage")),
):
    """
    Maintenance endpoint that deduplicates tags across all annotations.
    Removes duplicate (name, category) pairs while preserving one instance.
    Returns count of annotations that had duplicates removed. Staff-only.
    """
    try:
        logger.info("Starting maintenance: deduplicating tags across all annotations")

        from tagging_service import deduplicate_tags

        # Get all annotations with tags
        query = select(models.Annotation).where(models.Annotation.tags.isnot(None))
        result = await session.execute(query)
        annotations_with_tags = result.scalars().all()

        updated_count = 0
        total_removed = 0

        for annotation in annotations_with_tags:
            if not annotation.tags:
                continue

            try:
                tags_list = json.loads(annotation.tags)
            except (json.JSONDecodeError, ValueError):
                logger.warning(f"Invalid JSON in annotation {annotation.id} tags")
                continue

            if not isinstance(tags_list, list):
                continue

            # Deduplicate
            deduped = deduplicate_tags(tags_list)

            # Check if anything was actually removed
            if len(deduped) < len(tags_list):
                removed_count = len(tags_list) - len(deduped)
                logger.info(
                    f"Annotation {annotation.id}: removed {removed_count} duplicate tags"
                )
                total_removed += removed_count

                # Update annotation with deduped tags
                await db.update_annotation(
                    session,
                    annotation.id,
                    models.AnnotationUpdate(
                        tags=json.dumps(deduped, ensure_ascii=False)
                    ),
                )
                updated_count += 1

        logger.info(
            f"Maintenance completed: updated {updated_count} annotations, removed {total_removed} duplicate tags total"
        )

        return {
            "status": "success",
            "message": f"Deduplicated tags in {updated_count} annotations ({total_removed} duplicates removed)",
            "updated_count": updated_count,
            "total_removed": total_removed,
        }

    except Exception as e:
        logger.error(f"Error in deduplicate-tags maintenance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _verify_internal_token(request: Request) -> None:
    """Gate service-to-service endpoints (e.g. the CraftPilot RAG backend
    calling back into this API) behind a shared secret sent as
    'X-Internal-Token'. Unlike admin_routes._verify_secret, this fails
    CLOSED if INTERNAL_API_TOKEN is unset — this guards a full-corpus data
    dump, so a missing secret must not silently mean "open to anyone".
    """
    expected = os.getenv("INTERNAL_API_TOKEN", "")
    provided = request.headers.get("X-Internal-Token", "")
    if not expected or not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(status_code=403, detail="Invalid internal token")


@app.get("/api/export/corpus")
async def export_corpus(
    request: Request,
    craft: Optional[str] = None,
    only_transcribed: bool = True,
    only_salient: bool = False,
    session: AsyncSession = Depends(db.get_session),
    _: None = Depends(_verify_internal_token),
):
    """
    Bulk export of all annotations across all videos for RAG evaluation.

    Not a browser-facing endpoint — the frontend never calls this. It's
    consumed server-to-server by the RAG backend, so it's gated by a shared
    internal token rather than a per-user Moodle login.

    Query params:
      craft           – filter by craft domain (e.g. 'glassblowing')
      only_transcribed – skip annotations with no transcription (default True)
      only_salient     – restrict to is_salient=1 annotations (default False)

    Returns a single JSON document shaped for direct use as a retrieval corpus:
      {
        "export_timestamp": "...",
        "total_annotations": N,
        "filters": {...},
        "corpus": [
          {
            "id": 1,
            "video_id": 10,
            "video_filename": "GX010225_etirage.MP4",
            "start_time": 5.0,
            "end_time": 15.0,
            "duration": 10.0,
            "transcription": "...",
            "craft": "glassblowing",
            "detected_task": "...",
            "detected_task_confidence": 0.87,
            "tags": [{"name": "...", "category": "..."}],
            "is_salient": true,
            "review_results": {...},
            "judge_decision": {...},
            "feedback": 1,
            "created_at": "..."
          },
          ...
        ]
      }
    """
    try:
        videos = await db.get_all_videos(session, skip=0, limit=10000)
        corpus = []

        for video in videos:
            video_id = video.id
            annotations = await db.get_annotations_by_video(session, video_id, skip=0, limit=10000)
            for ann in annotations:
                if only_transcribed and not ann.transcription:
                    continue
                if only_salient and not getattr(ann, "is_salient", 0):
                    continue
                if craft and getattr(ann, "craft", None) != craft:
                    continue

                def _json_or_none(val):
                    if val is None:
                        return None
                    try:
                        return json.loads(str(val))
                    except (json.JSONDecodeError, TypeError):
                        return val

                corpus.append({
                    "id": ann.id,
                    "video_id": video_id,
                    "video_filename": video.filename,
                    "start_time": ann.start_time,
                    "end_time": ann.end_time,
                    "duration": ann.end_time - ann.start_time,
                    "transcription": ann.transcription,
                    "transcription_status": getattr(ann, "transcription_status", None),
                    "craft": getattr(ann, "craft", None),
                    "detected_task": getattr(ann, "detected_task", None),
                    "detected_task_confidence": getattr(ann, "detected_task_confidence", None),
                    "tags": _json_or_none(getattr(ann, "tags", None)),
                    "is_salient": bool(getattr(ann, "is_salient", 0)),
                    "judge_decision": _json_or_none(getattr(ann, "judge_decision", None)),
                    "review_status": getattr(ann, "review_status", None),
                    "review_results": _json_or_none(getattr(ann, "review_results", None)),
                    "feedback": ann.feedback,
                    "created_at": ann.created_at.isoformat() if hasattr(ann.created_at, "isoformat") else ann.created_at,
                })

        export_data = {
            "export_timestamp": datetime.now(timezone.utc).isoformat(),
            "total_annotations": len(corpus),
            "filters": {
                "craft": craft,
                "only_transcribed": only_transcribed,
                "only_salient": only_salient,
            },
            "corpus": corpus,
        }

        from decimal import Decimal as _Decimal

        def _default_serializer(obj):
            if isinstance(obj, _Decimal):
                return float(obj)
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        export_filename = f"corpus_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        export_path = EXPORTS_DIR / export_filename

        async with aiofiles.open(export_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(export_data, indent=2, ensure_ascii=False, default=_default_serializer))

        logger.info(f"Corpus export created: {export_filename} ({len(corpus)} annotations)")

        return FileResponse(
            export_path, media_type="application/json", filename=export_filename
        )

    except Exception as e:
        logger.error(f"Error exporting corpus: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/export/{video_id}")
async def export_annotations(
    video_id: int,
    session: AsyncSession = Depends(db.get_session),
    current_user: MoodleUser = Depends(verify_moodle_jwt),
):
    """Export annotations for a video as JSON"""
    try:
        video = await db.get_video(session, video_id)
        if not video:
            raise HTTPException(status_code=404, detail="Video not found")
        _authorize_video_access(video, current_user)

        annotations = await db.get_annotations_by_video(session, video_id)

        # Build export data
        export_data = {
            "video_file": video.filename,
            "video_duration": video.duration,
            "annotation_count": len(annotations),
            "export_timestamp": datetime.now(timezone.utc).isoformat(),
            "annotations": [
                {
                    "id": ann.id,
                    "start_time": ann.start_time,
                    "end_time": ann.end_time,
                    "duration": ann.end_time - ann.start_time,
                    "transcription": ann.transcription,
                    "review_status": (
                        ann.review_status if hasattr(ann, "review_status") else None
                    ),
                    "review_results": (
                        json.loads(str(ann.review_results))
                        if hasattr(ann, "review_results")
                        and ann.review_results is not None
                        else None
                    ),
                    "is_salient": bool(getattr(ann, "is_salient", 0)),
                    "feedback": ann.feedback,
                    "feedback_choices": (
                        json.loads(str(ann.feedback_choices))
                        if bool(ann.feedback_choices)
                        else None
                    ),
                    "audio_file": ann.audio_filename,
                    "created_at": ann.created_at.isoformat(),
                }
                for ann in annotations
            ],
        }

        # Save export file
        export_filename = f"export_{video.filename}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        export_path = EXPORTS_DIR / export_filename

        async with aiofiles.open(export_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(export_data, indent=2, ensure_ascii=False))

        logger.info(f"Export created: {export_filename}")

        return FileResponse(
            export_path, media_type="application/json", filename=export_filename
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting annotations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Custom StaticFiles with no-cache headers for development
class NoCacheStaticFiles(StaticFiles):
    """Static files with no-cache headers to prevent browser caching during development"""

    async def get_response(self, path: str, scope: Scope) -> Response:
        response = await super().get_response(path, scope)
        # Add no-cache headers for CSS, JS, and HTML files
        if path.endswith((".css", ".js", ".html")):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response


# Mount static files (frontend) with no-cache for development
app.mount("/static", NoCacheStaticFiles(directory=str(FRONTEND_DIR)), name="static")


# ==================== TUTORIAL SEEN FLAG ====================

TUTORIAL_PREF_KEY = 'local_videoelicit_tutorial_seen'


@app.get("/api/tutorial-status")
async def get_tutorial_status(current_user: MoodleUser = Depends(verify_moodle_jwt)):
    """Return whether the current user has already seen the tutorial."""
    value = await moodle_db.get_user_pref(current_user.userid, TUTORIAL_PREF_KEY)
    return {"seen": value == "1"}


@app.post("/api/tutorial-seen")
async def mark_tutorial_seen(current_user: MoodleUser = Depends(verify_moodle_jwt)):
    """Persist that the current user has seen and closed the tutorial."""
    await moodle_db.set_user_pref(current_user.userid, TUTORIAL_PREF_KEY, "1")
    return {"ok": True}


# Serve index.html at root with no-cache headers
@app.get("/")
async def serve_index():
    """Serve the main index.html file with no-cache headers"""
    response = FileResponse(FRONTEND_DIR / "index.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# Run the application
if __name__ == "__main__":
    import uvicorn

    logger.info(f"Starting server on {HOST}:{PORT}")
    logger.info(f"Open http://localhost:{PORT} in your browser")

    uvicorn.run("main:app", host=HOST, port=PORT, reload=True, log_level="info")

"""
Compatibility wrapper for moodle_db to match old database.py interface
This allows main.py to work without massive refactoring
"""

from typing import AsyncGenerator, List, Optional, Dict, Any
from moodle_db import moodle_db
from models import (
    Video, Annotation, Project, Tag, Task, VideoSegment,
    VideoCreate, AnnotationCreate, AnnotationUpdate,
    ProjectCreate, ProjectUpdate, TagCreate, TaskCreate,
    VideoSegmentCreate, VideoSegmentUpdate
)


# Fake session class since moodle_db doesn't use sessions
class FakeSession:
    """Placeholder session object for compatibility"""
    pass


class _AsyncSessionLocalCtx:
    """Async context manager that yields a FakeSession, mimicking AsyncSessionLocal()"""
    async def __aenter__(self):
        return FakeSession()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


def AsyncSessionLocal():
    """Drop-in for SQLAlchemy's AsyncSessionLocal() used as async context manager"""
    return _AsyncSessionLocalCtx()


# Type alias for compatibility with SQLAlchemy code
AsyncSession = FakeSession


async def get_session() -> AsyncGenerator[FakeSession, None]:
    """Compatibility function that yields a fake session"""
    yield FakeSession()


async def init_db():
    """Initialize database"""
    await moodle_db.init_db()


def _dict_to_model(data: Optional[Dict], model_class):
    """Convert dictionary to Pydantic model, mapping Moodle fields to model fields"""
    if not data:
        return None
    
    # Map Moodle field names to model field names
    field_map = {
        'timecreated': 'created_at',
        'timemodified': 'updated_at',
        'videoid': 'video_id',
        'userid': 'user_id',
        'starttime': 'start_time',
        'endtime': 'end_time',
        'audiofilepath': 'audio_filepath',
        'transcriptionstatus': 'transcription_status',
        'judgestatus': 'judge_status',
        'judgedecision': 'judge_decision',
        'taggingstatus': 'tagging_status',
        'reviewstatus': 'review_status',
        'reviewresults': 'review_results',
        'reviewattempts': 'review_attempts',
        'issalient': 'is_salient',
        'detectedtask': 'detected_task',
        'projectid': 'project_id',
        'batchposition': 'batch_position',
        'filesize': 'file_size',
        'mimetype': 'mime_type',
        'sourcetype': 'source_type',
        'externalurl': 'external_url',
        'fastapiid': 'fastapi_video_id',
        'feedbackchoices': 'feedback_choices',
        'thumbnailpath': 'thumbnail_path',
        'usagecount': 'usage_count',
        'ispublished': 'is_published',
        'displayname': 'display_name'
    }
    
    # Convert Unix timestamps to datetime strings (ISO format)
    mapped_data = {}
    for key, value in data.items():
        new_key = field_map.get(key, key)
        
        # Convert Unix timestamps to ISO strings
        if new_key in ['created_at', 'updated_at'] and isinstance(value, int):
            from datetime import datetime, timezone
            mapped_data[new_key] = datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
        else:
            mapped_data[new_key] = value
    # Special-case: map Moodle 'timecreated' -> API 'uploaded_at' for videos
    if model_class.__name__ == 'Video' and 'created_at' in mapped_data and 'uploaded_at' not in mapped_data:
        mapped_data['uploaded_at'] = mapped_data['created_at']

    # VideoSegment: DB stores 'videoid' but model uses 'parent_video_id'
    # (generic map sends 'videoid' -> 'video_id' for annotations; override for segments)
    if model_class.__name__ == 'VideoSegment' and 'video_id' in mapped_data and 'parent_video_id' not in mapped_data:
        mapped_data['parent_video_id'] = mapped_data.pop('video_id')

    # Ensure sensible defaults for Video model when DB lacks certain columns
    if model_class.__name__ == 'Video':
        # 'is_local' default (Moodle table does not store this field)
        if 'is_local' not in mapped_data:
            mapped_data['is_local'] = 0
        # 'mime_type' may be stored as 'mimetype' in Moodle
        if 'mime_type' not in mapped_data and 'mimetype' in data:
            mapped_data['mime_type'] = data.get('mimetype')

    try:
        # Filter out any Moodle-only keys that are not model attributes
        try:
            allowed_fields = {c.name for c in model_class.__table__.columns}
            filtered = {k: v for k, v in mapped_data.items() if k in allowed_fields}
        except Exception:
            # If model_class is not a SQLAlchemy model, fall back to original mapped_data
            filtered = mapped_data

        return model_class(**filtered)
    except Exception as e:
        # If model creation fails, log and return the raw dict for graceful degradation
        print(f"Warning: Could not create {model_class.__name__} from dict: {e}")
        return data


def _model_to_dict(model, exclude_unset: bool = False):
    """Convert a Pydantic model to a dict for moodle_db.

    By default None values are dropped, which suits creates (absent means
    "use the default"). Partial updates need `exclude_unset=True` instead:
    there, an explicitly-supplied None is a real instruction — clearing a
    project's knowledge-silo cohort back to open access — and dropping it
    would make that choice silently do nothing, while a field the caller
    never mentioned is still left untouched.
    """
    if hasattr(model, 'model_dump'):
        if exclude_unset:
            return model.model_dump(exclude_unset=True)
        return model.model_dump(exclude_none=True)
    elif hasattr(model, 'dict'):
        if exclude_unset:
            return model.dict(exclude_unset=True)
        return model.dict(exclude_none=True)
    else:
        return model


# ==================== VIDEO OPERATIONS ====================

async def create_video(session: FakeSession, video_data: VideoCreate) -> Video:
    """Create new video"""
    data_dict = _model_to_dict(video_data)
    result = await moodle_db.create_video(data_dict)
    return _dict_to_model(result, Video)


async def get_video(session: FakeSession, video_id: int) -> Optional[Video]:
    """Get video by ID"""
    result = await moodle_db.get_video(video_id)
    return _dict_to_model(result, Video)


async def get_video_by_filepath(session: FakeSession, filepath: str) -> Optional[Video]:
    """Get video by filepath"""
    result = await moodle_db.get_video_by_filepath(filepath)
    return _dict_to_model(result, Video)


async def get_all_videos(session: FakeSession, skip: int = 0, limit: int = 100) -> List[Video]:
    """Get all videos"""
    results = await moodle_db.get_all_videos(skip, limit)
    return [_dict_to_model(r, Video) for r in results]


async def get_videos_by_user(session: FakeSession, userid: int, skip: int = 0, limit: int = 100) -> List[Video]:
    """Get videos belonging to a specific user"""
    results = await moodle_db.get_videos_by_user(userid, skip, limit)
    return [_dict_to_model(r, Video) for r in results]


async def delete_video(session: FakeSession, video_id: int) -> bool:
    """Delete video"""
    return await moodle_db.delete_video(video_id)


async def update_video(session: FakeSession, video_id: int, update_data: Dict[str, Any]) -> Optional[Video]:
    """Update video metadata"""
    result = await moodle_db.update_video(video_id, update_data)
    return _dict_to_model(result, Video)


# ==================== VIDEO SEGMENT OPERATIONS ====================

async def create_video_segment(session: FakeSession, segment_data: VideoSegmentCreate) -> VideoSegment:
    """Create new video segment"""
    data_dict = _model_to_dict(segment_data)
    result = await moodle_db.create_video_segment(data_dict)
    return _dict_to_model(result, VideoSegment)


async def get_video_segment(session: FakeSession, segment_id: int) -> Optional[VideoSegment]:
    """Get video segment by ID"""
    result = await moodle_db.get_video_segment(segment_id)
    return _dict_to_model(result, VideoSegment)


async def get_video_segments(session: FakeSession, video_id: int) -> List[VideoSegment]:
    """Get all segments for a video"""
    results = await moodle_db.get_video_segments(video_id)
    return [_dict_to_model(r, VideoSegment) for r in results]


async def update_video_segment(session: FakeSession, segment_id: int, update_data: VideoSegmentUpdate) -> Optional[VideoSegment]:
    """Update video segment"""
    data_dict = _model_to_dict(update_data)
    result = await moodle_db.update_video_segment(segment_id, data_dict)
    return _dict_to_model(result, VideoSegment)


async def delete_video_segment(session: FakeSession, segment_id: int) -> bool:
    """Delete video segment"""
    return await moodle_db.delete_video_segment(segment_id)


# ==================== ANNOTATION OPERATIONS ====================

async def create_annotation(session: FakeSession, annotation_data: AnnotationCreate) -> Annotation:
    """Create new annotation"""
    data_dict = _model_to_dict(annotation_data)
    result = await moodle_db.create_annotation(data_dict)
    return _dict_to_model(result, Annotation)


async def get_annotation(session: FakeSession, annotation_id: int) -> Optional[Annotation]:
    """Get annotation by ID"""
    result = await moodle_db.get_annotation(annotation_id)
    return _dict_to_model(result, Annotation)


async def get_annotations_by_video(session: FakeSession, video_id: int, skip: int = 0, limit: int = 100) -> List[Annotation]:
    """Get all annotations for a video"""
    results = await moodle_db.get_annotations_by_video(video_id, skip, limit)
    return [_dict_to_model(r, Annotation) for r in results]


async def update_annotation(session: FakeSession, annotation_id: int, update_data: AnnotationUpdate) -> Optional[Annotation]:
    """Update annotation (critical for AI pipeline)"""
    data_dict = _model_to_dict(update_data)
    result = await moodle_db.update_annotation(annotation_id, data_dict)
    return _dict_to_model(result, Annotation)


async def delete_annotation(session: FakeSession, annotation_id: int) -> bool:
    """Delete annotation"""
    return await moodle_db.delete_annotation(annotation_id)


async def get_annotation_count(session: FakeSession, video_id: int) -> int:
    """Get count of annotations for a video"""
    return await moodle_db.get_annotation_count(video_id)


async def get_pending_transcriptions(session: FakeSession, limit: int = 10) -> List[Annotation]:
    """Get annotations pending transcription"""
    results = await moodle_db.get_pending_transcriptions(limit)
    return [_dict_to_model(r, Annotation) for r in results]


# ==================== PROJECT OPERATIONS ====================

async def create_project(session: FakeSession, project_data: ProjectCreate) -> Project:
    """Create new project"""
    data_dict = _model_to_dict(project_data)
    result = await moodle_db.create_project(data_dict)
    return _dict_to_model(result, Project)


async def get_project(session: FakeSession, project_id: int) -> Optional[Project]:
    """Get project by ID"""
    result = await moodle_db.get_project(project_id)
    return _dict_to_model(result, Project)


async def get_all_projects(session: FakeSession, skip: int = 0, limit: int = 100) -> List[Project]:
    """Get all projects"""
    results = await moodle_db.get_all_projects(skip, limit)
    return [_dict_to_model(r, Project) for r in results]


async def update_project(session: FakeSession, project_id: int, update_data: ProjectUpdate) -> Optional[Project]:
    """Update project"""
    # exclude_unset: an explicit allowed_cohort_id=None means "open access"
    # and must reach the database, not be stripped as a missing value.
    data_dict = _model_to_dict(update_data, exclude_unset=True)
    result = await moodle_db.update_project(project_id, data_dict)
    return _dict_to_model(result, Project)


async def delete_project(session: FakeSession, project_id: int) -> bool:
    """Delete project"""
    return await moodle_db.delete_project(project_id)


async def get_videos_by_project(session: FakeSession, project_id: int, skip: int = 0, limit: int = 100) -> List[Video]:
    """Get all videos in a project"""
    results = await moodle_db.get_videos_by_project(project_id, skip, limit)
    return [_dict_to_model(r, Video) for r in results]


# ==================== TAG OPERATIONS ====================

async def create_tag(session: FakeSession, tag_data: TagCreate) -> Tag:
    """Create new tag"""
    data_dict = _model_to_dict(tag_data)
    result = await moodle_db.create_tag(data_dict)
    return _dict_to_model(result, Tag)


async def get_tag_by_name(session: FakeSession, name: str) -> Optional[Tag]:
    """Get tag by name"""
    result = await moodle_db.get_tag_by_name(name)
    return _dict_to_model(result, Tag)


async def get_all_tags(session: FakeSession, skip: int = 0, limit: int = 1000) -> List[Tag]:
    """Get all tags"""
    results = await moodle_db.get_all_tags(skip, limit)
    return [_dict_to_model(r, Tag) for r in results]


async def increment_tag_usage(session: FakeSession, tag_name: str) -> Optional[Tag]:
    """Increment tag usage count"""
    result = await moodle_db.increment_tag_usage(tag_name)
    return _dict_to_model(result, Tag)


async def get_or_create_tag(session: FakeSession, name: str, category: str) -> Tag:
    """Get existing tag or create new one"""
    result = await moodle_db.get_or_create_tag(name, category)
    return _dict_to_model(result, Tag)


# ==================== TASK OPERATIONS ====================

async def create_task(session: FakeSession, task_data: TaskCreate) -> Task:
    """Create new task"""
    data_dict = _model_to_dict(task_data)
    result = await moodle_db.create_task(data_dict)
    return _dict_to_model(result, Task)


async def get_task_by_name(session: FakeSession, name: str) -> Optional[Task]:
    """Get task by name"""
    result = await moodle_db.get_task_by_name(name)
    return _dict_to_model(result, Task)


async def get_tasks(session: FakeSession, craft: Optional[str] = None, published: Optional[bool] = None, skip: int = 0, limit: int = 1000) -> List[Task]:
    """Get tasks with optional filtering"""
    results = await moodle_db.get_tasks(craft, published, skip, limit)
    return [_dict_to_model(r, Task) for r in results]


async def delete_task(session: FakeSession, name: str, craft: str) -> bool:
    """Delete task"""
    return await moodle_db.delete_task(name, craft)


# NOTE: AsyncSessionLocal is defined above as a function returning _AsyncSessionLocalCtx.
# Do NOT reassign it here — the function definition is the correct drop-in.


async def get_custom_crafts_by_user(userid: str) -> list:
    """Return custom crafts for a user as dicts with craft_key and craft_label."""
    rows = await moodle_db.get_custom_crafts_by_user(userid)
    return [{'craft_key': r['craft_key'], 'craft_label': r['craft_label']} for r in rows]


async def create_custom_craft(userid: str, craft_label: str) -> dict:
    """Slugify label, insert row, return {craft_key, craft_label}.

    Security: craft_key is derived entirely server-side from the label;
    the client never supplies a raw key.
    """
    import re
    craft_key = re.sub(r'[^a-z0-9]+', '_', craft_label.lower()).strip('_')
    if not craft_key:
        raise ValueError('craft_label produces an empty key after slugification')
    row = await moodle_db.create_custom_craft({
        'userid': userid,
        'craft_key': craft_key,
        'craft_label': craft_label,
    })
    return {'craft_key': row['craft_key'], 'craft_label': row['craft_label']}


async def create_session_advisory(
    video_id: int,
    user_id: Optional[int],
    suggestions: str,
    concerns: str,
    advisory_flag: str,
    guard_verdict: str,
) -> dict:
    """Persist an AI-generated session advisory (suggestions/concerns are JSON strings)."""
    return await moodle_db.create_session_advisory({
        'video_id': video_id,
        'user_id': user_id,
        'suggestions': suggestions,
        'concerns': concerns,
        'advisory_flag': advisory_flag,
        'guard_verdict': guard_verdict,
    })


async def get_session_advisory(advisory_id: int) -> Optional[dict]:
    """Get a session advisory by ID."""
    return await moodle_db.get_session_advisory(advisory_id)


async def record_advisory_decision(advisory_id: int, decision: str) -> Optional[dict]:
    """Record the user's continue/end decision against a session advisory."""
    return await moodle_db.record_advisory_decision(advisory_id, decision)

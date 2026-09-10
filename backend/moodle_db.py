"""
Moodle Database Adapter - Complete replacement for SQLAlchemy
Uses direct SQL with psycopg2 wrapped in async executors
"""

import os
import json
import asyncio
import threading
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

# Load .env early so MOODLE_DB_TYPE is available at import time
try:
    load_dotenv()
except Exception:
    pass  # .env may be missing or unreadable in test environments

# Import appropriate database driver based on environment
DB_TYPE = os.getenv('MOODLE_DB_TYPE', 'postgresql')

if DB_TYPE == 'postgresql':
    import psycopg2
    import psycopg2.pool
    from psycopg2.extras import RealDictCursor
elif DB_TYPE == 'mysql':
    import pymysql
    import pymysql.cursors as cursors_module
else:
    raise ValueError(f"Unsupported database type: {DB_TYPE}")


# Thread pool for async wrapper
executor = ThreadPoolExecutor(max_workers=10)


class _CompatConnection:
    """Wrap DB connection so callers can pass psycopg2-style cursor_factory kw for compatibility with pymysql."""
    def __init__(self, conn):
        self._conn = conn

    def cursor(self, *args, **kwargs):
        # Accept and ignore psycopg2-specific 'cursor_factory' kw
        kwargs.pop('cursor_factory', None)
        return self._conn.cursor(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class MoodleDBAdapter:
    """Singleton adapter for Moodle database operations"""
    
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize_pool()
        return cls._instance
    
    def _initialize_pool(self):
        """Initialize connection pool"""
        config = {
            'host': os.getenv('MOODLE_DB_HOST', 'localhost'),
            'database': os.getenv('MOODLE_DB_NAME', 'moodle'),
            'user': os.getenv('MOODLE_DB_USER', 'moodle'),
            'password': os.getenv('MOODLE_DB_PASSWORD', ''),
            'port': int(os.getenv('MOODLE_DB_PORT', '5432' if DB_TYPE == 'postgresql' else '3306'))
        }

        self.table_prefix = os.getenv('MOODLE_TABLE_PREFIX', 'mdl_')

        self._pool_lock = threading.Lock()

        if DB_TYPE == 'postgresql':
            try:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=2,
                    maxconn=10,
                    **config
                )
            except Exception:
                # Defer pool creation; pool will be None until DB is reachable.
                # This allows module import and unit-test instantiation without a live DB.
                self._pool = None
                self._pg_config = config
        elif DB_TYPE == 'mysql':
            # MySQL - simplified connection (no pooling for now)
            self._config = config
    
    @contextmanager
    def get_connection(self):
        """Get database connection from pool"""
        if DB_TYPE == 'postgresql':
            if self._pool is None:
                with self._pool_lock:
                    if self._pool is None:  # double-checked locking
                        # Lazy pool creation (pool was deferred at startup due to DB unavailability)
                        self._pool = psycopg2.pool.ThreadedConnectionPool(
                            minconn=2,
                            maxconn=10,
                            **self._pg_config
                        )
            conn = self._pool.getconn()
            try:
                yield conn
            finally:
                self._pool.putconn(conn)
        elif DB_TYPE == 'mysql':
            conn = pymysql.connect(
                **self._config,
                cursorclass=cursors_module.DictCursor
            )
            wrapped = _CompatConnection(conn)
            try:
                yield wrapped
            finally:
                conn.close()
    
    def _table(self, name: str) -> str:
        """Get full table name with Moodle prefix"""
        return f"{self.table_prefix}local_videoelicit_{name}"

    def _insert(self, cursor, query: str, params: tuple) -> int:
        """Execute an INSERT and return the new row id.
        PostgreSQL: uses RETURNING id clause.
        MySQL:      strips RETURNING id, uses cursor.lastrowid.
        """
        if DB_TYPE == 'postgresql':
            cursor.execute(query, params)
            return cursor.fetchone()[0]
        else:
            # Strip the PostgreSQL-only RETURNING clause before executing
            clean = query.replace('RETURNING id', '').strip().rstrip(';')
            cursor.execute(clean, params)
            return cursor.lastrowid
    
    async def _run_in_executor(self, func, *args):
        """Wrap synchronous DB call in async executor"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(executor, func, *args)
    
    # ==================== INIT ====================
    
    def init_db_sync(self):
        """Initialize database (no-op for Moodle - schema managed by upgrade.php)"""
        pass
    
    async def init_db(self):
        """Async wrapper for init_db"""
        return await self._run_in_executor(self.init_db_sync)

    def ensure_crafts_table_sync(self):
        """Create the custom crafts table if it doesn't exist yet."""
        table = self._table('crafts')
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if DB_TYPE == 'postgresql':
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        id          SERIAL PRIMARY KEY,
                        userid      VARCHAR(255) NOT NULL,
                        craft_key   VARCHAR(100) NOT NULL,
                        craft_label VARCHAR(255) NOT NULL,
                        timecreated INT NOT NULL DEFAULT 0,
                        UNIQUE (userid, craft_key)
                    )
                """)
            else:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        id          INT AUTO_INCREMENT PRIMARY KEY,
                        userid      VARCHAR(255) NOT NULL,
                        craft_key   VARCHAR(100) NOT NULL,
                        craft_label VARCHAR(255) NOT NULL,
                        timecreated INT NOT NULL DEFAULT 0,
                        UNIQUE KEY uq_user_craft (userid, craft_key)
                    )
                """)
            conn.commit()

    async def ensure_crafts_table(self):
        return await self._run_in_executor(self.ensure_crafts_table_sync)

    def ensure_session_advisories_table_sync(self):
        """Create the session advisories table if it doesn't exist yet."""
        table = self._table('session_advisories')
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if DB_TYPE == 'postgresql':
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        id            SERIAL PRIMARY KEY,
                        videoid       INT NOT NULL,
                        userid        VARCHAR(255),
                        suggestions   TEXT,
                        concerns      TEXT,
                        advisory_flag VARCHAR(20),
                        guard_verdict VARCHAR(20),
                        user_decision VARCHAR(20),
                        decided_at    INT,
                        timecreated   INT NOT NULL DEFAULT 0
                    )
                """)
            else:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        id            INT AUTO_INCREMENT PRIMARY KEY,
                        videoid       INT NOT NULL,
                        userid        VARCHAR(255),
                        suggestions   TEXT,
                        concerns      TEXT,
                        advisory_flag VARCHAR(20),
                        guard_verdict VARCHAR(20),
                        user_decision VARCHAR(20),
                        decided_at    INT,
                        timecreated   INT NOT NULL DEFAULT 0
                    )
                """)
            conn.commit()

    async def ensure_session_advisories_table(self):
        return await self._run_in_executor(self.ensure_session_advisories_table_sync)

    # ==================== CUSTOM CRAFTS ====================

    def get_custom_crafts_by_user_sync(self, userid: str) -> List[Dict[str, Any]]:
        """Return all custom crafts for a user, ordered by creation time."""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(
                f"SELECT id, userid, craft_key, craft_label, timecreated"
                f" FROM {self._table('crafts')} WHERE userid = %s ORDER BY timecreated ASC",
                (userid,)
            )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]

    async def get_custom_crafts_by_user(self, userid: str) -> List[Dict[str, Any]]:
        return await self._run_in_executor(self.get_custom_crafts_by_user_sync, userid)

    def create_custom_craft_sync(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a new custom craft row. Raises IntegrityError on duplicate (userid, craft_key)."""
        table = self._table('crafts')
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            now = int(datetime.now(timezone.utc).timestamp())
            query = f"""
                INSERT INTO {table} (userid, craft_key, craft_label, timecreated)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """
            row_id = self._insert(cursor, query, (
                data['userid'],
                data['craft_key'],
                data['craft_label'],
                now,
            ))
            conn.commit()
            cursor.execute(
                f"SELECT id, userid, craft_key, craft_label, timecreated FROM {table} WHERE id = %s",
                (row_id,)
            )
            row = cursor.fetchone()
            return dict(row)

    async def create_custom_craft(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return await self._run_in_executor(self.create_custom_craft_sync, data)

    # ==================== SESSION ADVISORIES ====================

    def create_session_advisory_sync(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a new AI session advisory row and return it."""
        table = self._table('session_advisories')
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            now = int(datetime.now(timezone.utc).timestamp())
            query = f"""
                INSERT INTO {table}
                (videoid, userid, suggestions, concerns, advisory_flag, guard_verdict, timecreated)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            row_id = self._insert(cursor, query, (
                data['video_id'],
                data.get('user_id'),
                data['suggestions'],
                data['concerns'],
                data['advisory_flag'],
                data['guard_verdict'],
                now,
            ))
            conn.commit()
            cursor.execute(f"SELECT * FROM {table} WHERE id = %s", (row_id,))
            row = cursor.fetchone()
            return dict(row)

    async def create_session_advisory(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return await self._run_in_executor(self.create_session_advisory_sync, data)

    def get_session_advisory_sync(self, advisory_id: int) -> Optional[Dict[str, Any]]:
        """Get a session advisory by ID."""
        table = self._table('session_advisories')
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(f"SELECT * FROM {table} WHERE id = %s", (advisory_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    async def get_session_advisory(self, advisory_id: int) -> Optional[Dict[str, Any]]:
        return await self._run_in_executor(self.get_session_advisory_sync, advisory_id)

    def record_advisory_decision_sync(self, advisory_id: int, decision: str) -> Optional[Dict[str, Any]]:
        """Set user_decision/decided_at on an existing session advisory row."""
        table = self._table('session_advisories')
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            now = int(datetime.now(timezone.utc).timestamp())
            cursor.execute(
                f"UPDATE {table} SET user_decision = %s, decided_at = %s WHERE id = %s",
                (decision, now, advisory_id),
            )
            conn.commit()
            cursor.execute(f"SELECT * FROM {table} WHERE id = %s", (advisory_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    async def record_advisory_decision(self, advisory_id: int, decision: str) -> Optional[Dict[str, Any]]:
        return await self._run_in_executor(self.record_advisory_decision_sync, advisory_id, decision)

    # ==================== VIDEOS ====================
    
    def create_video_sync(self, video_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create new video record"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)

            now = int(datetime.now(timezone.utc).timestamp())

            if DB_TYPE == 'postgresql':
                query = f"""
                    INSERT INTO {self._table('videos')}
                    (filename, filepath, filesize, source_type, external_url, fastapi_video_id,
                     userid, timecreated, timemodified, projectid)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """
                cursor.execute(query, (
                    video_data.get('filename'),
                    video_data.get('filepath'),
                    video_data.get('file_size', 0),
                    video_data.get('source_type', 'local'),
                    video_data.get('external_url'),
                    video_data.get('fastapi_video_id'),
                    video_data.get('user_id', 0),
                    now, now,
                    video_data.get('project_id'),
                ))
                video_id = cursor.fetchone()[0]
            else:
                # MySQL — no RETURNING, use lastrowid
                # Must also supply the NOT NULL columns required by the Moodle schema.
                query = f"""
                    INSERT INTO {self._table('videos')}
                    (contextid, userid, filename, fileitemid, filearea, filepath,
                     filesize, mimetype, source_type, external_url, fastapi_video_id,
                     timecreated, timemodified, projectid)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query, (
                    video_data.get('contextid', 1),
                    video_data.get('user_id', 0),
                    video_data.get('filename'),
                    video_data.get('fileitemid', 0),
                    video_data.get('filearea', 'videos'),
                    video_data.get('filepath', '/'),
                    video_data.get('file_size', 0),
                    video_data.get('mime_type', 'video/mp4'),
                    video_data.get('source_type', 'local'),
                    video_data.get('external_url'),
                    video_data.get('fastapi_video_id'),
                    now, now,
                    video_data.get('project_id'),
                ))
                video_id = cursor.lastrowid

            conn.commit()

            # Return created video — use a fresh dict cursor for MySQL
            if DB_TYPE == 'postgresql':
                cursor.execute(f"SELECT * FROM {self._table('videos')} WHERE id = %s", (video_id,))
                return dict(cursor.fetchone())
            else:
                dict_cursor = conn.cursor()
                dict_cursor.execute(f"SELECT * FROM {self._table('videos')} WHERE id = %s", (video_id,))
                row = dict_cursor.fetchone()
                # pymysql DictCursor returns a dict; normal cursor returns a tuple.
                if not row:
                    return {}
                if isinstance(row, dict):
                    return row
                # tuple result -> build dict from description
                cols = [d[0] for d in dict_cursor.description]
                return dict(zip(cols, row))
    
    async def create_video(self, video_data: Dict[str, Any]) -> Dict[str, Any]:
        """Async wrapper for create_video"""
        return await self._run_in_executor(self.create_video_sync, video_data)
    
    def get_video_sync(self, video_id: int) -> Optional[Dict[str, Any]]:
        """Get video by ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(f"SELECT * FROM {self._table('videos')} WHERE id = %s", (video_id,))
            result = cursor.fetchone()
            return dict(result) if result else None
    
    async def get_video(self, video_id: int) -> Optional[Dict[str, Any]]:
        """Async wrapper for get_video"""
        return await self._run_in_executor(self.get_video_sync, video_id)
    
    def get_video_by_filepath_sync(self, filepath: str) -> Optional[Dict[str, Any]]:
        """Get video by filepath"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(f"SELECT * FROM {self._table('videos')} WHERE filepath = %s", (filepath,))
            result = cursor.fetchone()
            return dict(result) if result else None
    
    async def get_video_by_filepath(self, filepath: str) -> Optional[Dict[str, Any]]:
        """Async wrapper for get_video_by_filepath"""
        return await self._run_in_executor(self.get_video_by_filepath_sync, filepath)
    
    def get_all_videos_sync(self, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all videos with pagination"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(
                f"SELECT * FROM {self._table('videos')} ORDER BY timemodified DESC LIMIT %s OFFSET %s",
                (limit, skip)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    async def get_all_videos(self, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Async wrapper for get_all_videos"""
        return await self._run_in_executor(self.get_all_videos_sync, skip, limit)

    def get_videos_by_user_sync(self, userid: int, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Get videos belonging to a specific user"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(
                f"SELECT * FROM {self._table('videos')} WHERE userid = %s ORDER BY timemodified DESC LIMIT %s OFFSET %s",
                (userid, limit, skip)
            )
            return [dict(row) for row in cursor.fetchall()]

    async def get_videos_by_user(self, userid: int, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Async wrapper for get_videos_by_user"""
        return await self._run_in_executor(self.get_videos_by_user_sync, userid, skip, limit)

    def delete_video_sync(self, video_id: int) -> bool:
        """Delete video and all its annotations"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Delete annotations first (foreign key constraint)
            cursor.execute(f"DELETE FROM {self._table('annotations')} WHERE videoid = %s", (video_id,))
            
            # Delete segments
            cursor.execute(f"DELETE FROM {self._table('segments')} WHERE videoid = %s", (video_id,))
            
            # Delete video
            cursor.execute(f"DELETE FROM {self._table('videos')} WHERE id = %s", (video_id,))
            rows_deleted = cursor.rowcount
            
            conn.commit()
            return rows_deleted > 0
    
    async def delete_video(self, video_id: int) -> bool:
        """Async wrapper for delete_video"""
        return await self._run_in_executor(self.delete_video_sync, video_id)

    def update_video_sync(self, video_id: int, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update video metadata. Only whitelisted fields are written."""
        # Map model field names -> DB column names
        column_map = {
            'display_name': 'displayname',
            'displayname': 'displayname',
            'project_id': 'projectid',
            'projectid': 'projectid',
            'batch_position': 'batchposition',
            'batchposition': 'batchposition',
        }

        sets = []
        params: List[Any] = []
        for key, value in update_data.items():
            col = column_map.get(key)
            if not col:
                continue
            sets.append(f"{col} = %s")
            params.append(value)

        if not sets:
            return self.get_video_sync(video_id)

        sets.append("timemodified = %s")
        params.append(int(datetime.now(timezone.utc).timestamp()))
        params.append(video_id)

        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            query = f"UPDATE {self._table('videos')} SET {', '.join(sets)} WHERE id = %s"
            cursor.execute(query, tuple(params))
            conn.commit()

        return self.get_video_sync(video_id)

    async def update_video(self, video_id: int, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Async wrapper for update_video"""
        return await self._run_in_executor(self.update_video_sync, video_id, update_data)

    # ==================== VIDEO SEGMENTS ====================
    
    def create_video_segment_sync(self, segment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create new video segment"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)

            now = int(datetime.now(timezone.utc).timestamp())

            if DB_TYPE == 'postgresql':
                query = f"""
                    INSERT INTO {self._table('segments')}
                    (videoid, name, starttime, endtime, thumbnailpath, timecreated, timemodified)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """
                cursor.execute(query, (
                    segment_data['parent_video_id'],
                    segment_data.get('name'),
                    segment_data['start_time'],
                    segment_data['end_time'],
                    segment_data.get('thumbnail_path'),
                    now, now
                ))
                segment_id = cursor.fetchone()[0]
            else:
                query = f"""
                    INSERT INTO {self._table('segments')}
                    (videoid, name, starttime, endtime, thumbnailpath, timecreated, timemodified)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query, (
                    segment_data['parent_video_id'],
                    segment_data.get('name'),
                    segment_data['start_time'],
                    segment_data['end_time'],
                    segment_data.get('thumbnail_path'),
                    now, now
                ))
                segment_id = cursor.lastrowid

            conn.commit()

            cursor.execute(f"SELECT * FROM {self._table('segments')} WHERE id = %s", (segment_id,))
            return dict(cursor.fetchone())
    
    async def create_video_segment(self, segment_data: Dict[str, Any]) -> Dict[str, Any]:
        """Async wrapper for create_video_segment"""
        return await self._run_in_executor(self.create_video_segment_sync, segment_data)
    
    def get_video_segment_sync(self, segment_id: int) -> Optional[Dict[str, Any]]:
        """Get video segment by ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(f"SELECT * FROM {self._table('segments')} WHERE id = %s", (segment_id,))
            result = cursor.fetchone()
            return dict(result) if result else None
    
    async def get_video_segment(self, segment_id: int) -> Optional[Dict[str, Any]]:
        """Async wrapper for get_video_segment"""
        return await self._run_in_executor(self.get_video_segment_sync, segment_id)
    
    def get_video_segments_sync(self, video_id: int) -> List[Dict[str, Any]]:
        """Get all segments for a video"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(
                f"SELECT * FROM {self._table('segments')} WHERE videoid = %s ORDER BY starttime",
                (video_id,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    async def get_video_segments(self, video_id: int) -> List[Dict[str, Any]]:
        """Async wrapper for get_video_segments"""
        return await self._run_in_executor(self.get_video_segments_sync, video_id)
    
    def update_video_segment_sync(self, segment_id: int, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update video segment"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            
            now = int(datetime.now(timezone.utc).timestamp())
            
            # Build dynamic update query
            fields = []
            values = []
            
            if 'name' in update_data:
                fields.append("name = %s")
                values.append(update_data['name'])
            if 'start_time' in update_data:
                fields.append("starttime = %s")
                values.append(update_data['start_time'])
            if 'end_time' in update_data:
                fields.append("endtime = %s")
                values.append(update_data['end_time'])
            if 'thumbnail_path' in update_data:
                fields.append("thumbnailpath = %s")
                values.append(update_data['thumbnail_path'])
            
            fields.append("timemodified = %s")
            values.append(now)
            values.append(segment_id)
            
            if not fields:
                return self.get_video_segment_sync(segment_id)
            
            query = f"UPDATE {self._table('segments')} SET {', '.join(fields)} WHERE id = %s"
            cursor.execute(query, values)
            conn.commit()
            
            return self.get_video_segment_sync(segment_id)
    
    async def update_video_segment(self, segment_id: int, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Async wrapper for update_video_segment"""
        return await self._run_in_executor(self.update_video_segment_sync, segment_id, update_data)
    
    def delete_video_segment_sync(self, segment_id: int) -> bool:
        """Delete video segment"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {self._table('segments')} WHERE id = %s", (segment_id,))
            rows_deleted = cursor.rowcount
            conn.commit()
            return rows_deleted > 0
    
    async def delete_video_segment(self, segment_id: int) -> bool:
        """Async wrapper for delete_video_segment"""
        return await self._run_in_executor(self.delete_video_segment_sync, segment_id)
    
    # ==================== ANNOTATIONS ====================
    
    def create_annotation_sync(self, annotation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create new annotation"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)

            now = int(datetime.now(timezone.utc).timestamp())

            query = f"""
                INSERT INTO {self._table('annotations')}
                (videoid, contextid, audiofileitemid, starttime, endtime, audiofilepath,
                 transcription, transcriptionstatus,
                 userid, timecreated, timemodified, craft, task,
                 judgestatus, taggingstatus, reviewstatus)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """

            annotation_id = self._insert(cursor, query, (
                annotation_data['video_id'],
                annotation_data.get('context_id', 0),
                0,  # audiofileitemid — file stored on disk, not in Moodle filestore
                annotation_data['start_time'],
                annotation_data['end_time'],
                annotation_data.get('audio_filepath'),
                annotation_data.get('transcription'),
                annotation_data.get('transcription_status', 'pending'),
                annotation_data.get('user_id', 0),
                now, now,
                annotation_data.get('craft'),
                annotation_data.get('task'),
                'pending',  # judge_status
                'pending',  # tagging_status
                'pending',  # review_status
            ))
            conn.commit()

            cursor.execute(f"SELECT * FROM {self._table('annotations')} WHERE id = %s", (annotation_id,))
            return dict(cursor.fetchone())
    
    async def create_annotation(self, annotation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Async wrapper for create_annotation"""
        return await self._run_in_executor(self.create_annotation_sync, annotation_data)
    
    def get_annotation_sync(self, annotation_id: int) -> Optional[Dict[str, Any]]:
        """Get annotation by ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(f"SELECT * FROM {self._table('annotations')} WHERE id = %s", (annotation_id,))
            result = cursor.fetchone()
            return dict(result) if result else None
    
    async def get_annotation(self, annotation_id: int) -> Optional[Dict[str, Any]]:
        """Async wrapper for get_annotation"""
        return await self._run_in_executor(self.get_annotation_sync, annotation_id)
    
    def get_annotations_by_video_sync(self, video_id: int, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all annotations for a video"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(
                f"SELECT * FROM {self._table('annotations')} WHERE videoid = %s ORDER BY starttime LIMIT %s OFFSET %s",
                (video_id, limit, skip)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    async def get_annotations_by_video(self, video_id: int, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Async wrapper for get_annotations_by_video"""
        return await self._run_in_executor(self.get_annotations_by_video_sync, video_id, skip, limit)
    
    def update_annotation_sync(self, annotation_id: int, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update annotation (critical for AI pipeline)"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            
            now = int(datetime.now(timezone.utc).timestamp())
            
            # Build dynamic update query
            fields = []
            values = []
            
            # Map snake_case to Moodle field names
            field_mapping = {
                'transcription': 'transcription',
                'transcription_status': 'transcriptionstatus',
                'language': 'language',
                'audio_filepath': 'audiofilepath',
                'judge_status': 'judgestatus',
                'judge_decision': 'judgedecision',
                'tagging_status': 'taggingstatus',
                'tags': 'tags',
                'review_status': 'reviewstatus',
                'review_results': 'reviewresults',
                'review_attempts': 'reviewattempts',
                'is_salient': 'issalient',
                'detected_task': 'detectedtask',
                'craft': 'craft',
                'task': 'task',
                'feedback': 'feedback',
                'feedback_choices': 'feedbackchoices'
            }
            
            for key, value in update_data.items():
                if key in field_mapping:
                    db_field = field_mapping[key]
                    fields.append(f"{db_field} = %s")
                    
                    # Serialize dicts/lists to JSON
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value)
                    
                    values.append(value)
            
            if not fields:
                return self.get_annotation_sync(annotation_id)
            
            fields.append("timemodified = %s")
            values.append(now)
            values.append(annotation_id)
            
            query = f"UPDATE {self._table('annotations')} SET {', '.join(fields)} WHERE id = %s"
            cursor.execute(query, values)
            conn.commit()
            
            return self.get_annotation_sync(annotation_id)
    
    async def update_annotation(self, annotation_id: int, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Async wrapper for update_annotation"""
        return await self._run_in_executor(self.update_annotation_sync, annotation_id, update_data)
    
    def delete_annotation_sync(self, annotation_id: int) -> bool:
        """Delete annotation"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {self._table('annotations')} WHERE id = %s", (annotation_id,))
            rows_deleted = cursor.rowcount
            conn.commit()
            return rows_deleted > 0
    
    async def delete_annotation(self, annotation_id: int) -> bool:
        """Async wrapper for delete_annotation"""
        return await self._run_in_executor(self.delete_annotation_sync, annotation_id)
    
    def get_annotation_count_sync(self, video_id: int) -> int:
        """Get count of annotations for a video"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) AS cnt FROM {self._table('annotations')} WHERE videoid = %s", (video_id,))
            row = cursor.fetchone()
            # DictCursor returns a dict; plain cursor returns a tuple
            return row['cnt'] if isinstance(row, dict) else row[0]
    
    async def get_annotation_count(self, video_id: int) -> int:
        """Async wrapper for get_annotation_count"""
        return await self._run_in_executor(self.get_annotation_count_sync, video_id)
    
    def get_pending_transcriptions_sync(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get annotations pending transcription"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(
                f"SELECT * FROM {self._table('annotations')} WHERE transcriptionstatus = 'pending' LIMIT %s",
                (limit,)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    async def get_pending_transcriptions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Async wrapper for get_pending_transcriptions"""
        return await self._run_in_executor(self.get_pending_transcriptions_sync, limit)
    
    # ==================== PROJECTS ====================
    
    def create_project_sync(self, project_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create new project"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)

            now = int(datetime.now(timezone.utc).timestamp())

            query = f"""
                INSERT INTO {self._table('projects')}
                (name, description, allowed_cohort_id, userid, timecreated, timemodified)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """

            project_id = self._insert(cursor, query, (
                project_data['name'],
                project_data.get('description'),
                # Knowledge silo: None means open access.
                project_data.get('allowed_cohort_id'),
                project_data.get('userid', 0),
                now, now,
            ))
            conn.commit()

            cursor.execute(f"SELECT * FROM {self._table('projects')} WHERE id = %s", (project_id,))
            return dict(cursor.fetchone())
    
    def cohort_exists_sync(self, cohort_id: Optional[int]) -> bool:
        """Whether a cohort id refers to a real Moodle cohort.

        None is valid: it is the "open access" choice. A non-existent id must
        be rejected because content locked to it becomes retrievable by
        nobody — a silent failure that looks identical to a broken silo.
        """
        if cohort_id is None:
            return True
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM mdl_cohort WHERE id = %s", (cohort_id,))
            return cursor.fetchone() is not None

    async def cohort_exists(self, cohort_id: Optional[int]) -> bool:
        """Async wrapper for cohort_exists_sync"""
        return await self._run_in_executor(self.cohort_exists_sync, cohort_id)

    async def create_project(self, project_data: Dict[str, Any]) -> Dict[str, Any]:
        """Async wrapper for create_project"""
        return await self._run_in_executor(self.create_project_sync, project_data)
    
    def get_project_sync(self, project_id: int) -> Optional[Dict[str, Any]]:
        """Get project by ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(f"SELECT * FROM {self._table('projects')} WHERE id = %s", (project_id,))
            result = cursor.fetchone()
            return dict(result) if result else None
    
    async def get_project(self, project_id: int) -> Optional[Dict[str, Any]]:
        """Async wrapper for get_project"""
        return await self._run_in_executor(self.get_project_sync, project_id)
    
    def get_all_projects_sync(self, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all projects with pagination"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(
                f"SELECT * FROM {self._table('projects')} ORDER BY timemodified DESC LIMIT %s OFFSET %s",
                (limit, skip)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    async def get_all_projects(self, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Async wrapper for get_all_projects"""
        return await self._run_in_executor(self.get_all_projects_sync, skip, limit)
    
    def update_project_sync(self, project_id: int, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update project"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            
            now = int(datetime.now(timezone.utc).timestamp())
            
            fields = []
            values = []
            
            if 'name' in update_data:
                fields.append("name = %s")
                values.append(update_data['name'])
            if 'description' in update_data:
                fields.append("description = %s")
                values.append(update_data['description'])
            # Membership test, not a truthiness test: an explicit None is the
            # "Open access" choice and must be persisted, not skipped.
            if 'allowed_cohort_id' in update_data:
                fields.append("allowed_cohort_id = %s")
                values.append(update_data['allowed_cohort_id'])
            
            if not fields:
                return self.get_project_sync(project_id)
            
            fields.append("timemodified = %s")
            values.append(now)
            values.append(project_id)
            
            query = f"UPDATE {self._table('projects')} SET {', '.join(fields)} WHERE id = %s"
            cursor.execute(query, values)
            conn.commit()
            
            return self.get_project_sync(project_id)
    
    async def update_project(self, project_id: int, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Async wrapper for update_project"""
        return await self._run_in_executor(self.update_project_sync, project_id, update_data)
    
    def delete_project_sync(self, project_id: int) -> bool:
        """Delete project (unlinks videos, doesn't delete them)"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Unlink videos from project
            cursor.execute(f"UPDATE {self._table('videos')} SET projectid = NULL WHERE projectid = %s", (project_id,))
            
            # Delete project
            cursor.execute(f"DELETE FROM {self._table('projects')} WHERE id = %s", (project_id,))
            rows_deleted = cursor.rowcount
            
            conn.commit()
            return rows_deleted > 0
    
    async def delete_project(self, project_id: int) -> bool:
        """Async wrapper for delete_project"""
        return await self._run_in_executor(self.delete_project_sync, project_id)
    
    def get_videos_by_project_sync(self, project_id: int, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all videos in a project"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(
                f"SELECT * FROM {self._table('videos')} WHERE projectid = %s ORDER BY batchposition, timemodified DESC LIMIT %s OFFSET %s",
                (project_id, limit, skip)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    async def get_videos_by_project(self, project_id: int, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Async wrapper for get_videos_by_project"""
        return await self._run_in_executor(self.get_videos_by_project_sync, project_id, skip, limit)
    
    # ==================== TAGS ====================
    
    def create_tag_sync(self, tag_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create new tag"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)

            now = int(datetime.now(timezone.utc).timestamp())

            query = f"""
                INSERT INTO {self._table('tags')}
                (name, category, usagecount, timecreated)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """

            tag_id = self._insert(cursor, query, (
                tag_data['name'],
                tag_data['category'],
                tag_data.get('usage_count', 1),
                now,
            ))
            conn.commit()

            cursor.execute(f"SELECT * FROM {self._table('tags')} WHERE id = %s", (tag_id,))
            return dict(cursor.fetchone())
    
    async def create_tag(self, tag_data: Dict[str, Any]) -> Dict[str, Any]:
        """Async wrapper for create_tag"""
        return await self._run_in_executor(self.create_tag_sync, tag_data)
    
    def get_tag_by_name_sync(self, name: str) -> Optional[Dict[str, Any]]:
        """Get tag by name (category-agnostic)"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(f"SELECT * FROM {self._table('tags')} WHERE name = %s LIMIT 1", (name,))
            result = cursor.fetchone()
            return dict(result) if result else None
    
    async def get_tag_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Async wrapper for get_tag_by_name"""
        return await self._run_in_executor(self.get_tag_by_name_sync, name)
    
    def get_all_tags_sync(self, skip: int = 0, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get all tags with pagination"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(
                f"SELECT * FROM {self._table('tags')} ORDER BY usagecount DESC, name LIMIT %s OFFSET %s",
                (limit, skip)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    async def get_all_tags(self, skip: int = 0, limit: int = 1000) -> List[Dict[str, Any]]:
        """Async wrapper for get_all_tags"""
        return await self._run_in_executor(self.get_all_tags_sync, skip, limit)
    
    def increment_tag_usage_sync(self, tag_name: str) -> Optional[Dict[str, Any]]:
        """Increment usage count for a tag"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(
                f"UPDATE {self._table('tags')} SET usagecount = usagecount + 1 WHERE name = %s",
                (tag_name,)
            )
            conn.commit()
            return self.get_tag_by_name_sync(tag_name)
    
    async def increment_tag_usage(self, tag_name: str) -> Optional[Dict[str, Any]]:
        """Async wrapper for increment_tag_usage"""
        return await self._run_in_executor(self.increment_tag_usage_sync, tag_name)
    
    def get_or_create_tag_sync(self, name: str, category: str) -> Dict[str, Any]:
        """Get existing tag or create new one"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            
            # Try to get existing
            cursor.execute(
                f"SELECT * FROM {self._table('tags')} WHERE name = %s AND category = %s",
                (name, category)
            )
            result = cursor.fetchone()
            
            if result:
                return dict(result)
            
            # Create new
            now = int(datetime.now(timezone.utc).timestamp())
            tag_id = self._insert(
                cursor,
                f"INSERT INTO {self._table('tags')} (name, category, usagecount, timecreated, timemodified) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                (name, category, 1, now, now)
            )
            conn.commit()
            
            cursor.execute(f"SELECT * FROM {self._table('tags')} WHERE id = %s", (tag_id,))
            return dict(cursor.fetchone())
    
    async def get_or_create_tag(self, name: str, category: str) -> Dict[str, Any]:
        """Async wrapper for get_or_create_tag"""
        return await self._run_in_executor(self.get_or_create_tag_sync, name, category)
    
    # ==================== TASKS ====================
    
    def create_task_sync(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create new task"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)

            now = int(datetime.now(timezone.utc).timestamp())

            query = f"""
                INSERT INTO {self._table('tasks')}
                (name, craft, description, ispublished, timecreated)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """

            task_id = self._insert(cursor, query, (
                task_data['name'],
                task_data['craft'],
                task_data.get('description'),
                task_data.get('is_published', True),
                now,
            ))
            conn.commit()

            cursor.execute(f"SELECT * FROM {self._table('tasks')} WHERE id = %s", (task_id,))
            return dict(cursor.fetchone())
    
    async def create_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Async wrapper for create_task"""
        return await self._run_in_executor(self.create_task_sync, task_data)
    
    def get_task_by_name_sync(self, name: str) -> Optional[Dict[str, Any]]:
        """Get task by name"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(f"SELECT * FROM {self._table('tasks')} WHERE name = %s LIMIT 1", (name,))
            result = cursor.fetchone()
            return dict(result) if result else None
    
    async def get_task_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Async wrapper for get_task_by_name"""
        return await self._run_in_executor(self.get_task_by_name_sync, name)
    
    def get_tasks_sync(self, craft: Optional[str] = None, published: Optional[bool] = None, skip: int = 0, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get tasks with optional filtering"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            
            query = f"SELECT * FROM {self._table('tasks')} WHERE 1=1"
            params = []
            
            if craft:
                query += " AND craft = %s"
                params.append(craft)
            if published is not None:
                query += " AND ispublished = %s"
                params.append(1 if published else 0)
            
            query += " ORDER BY name LIMIT %s OFFSET %s"
            params.extend([limit, skip])
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    async def get_tasks(self, craft: Optional[str] = None, published: Optional[bool] = None, skip: int = 0, limit: int = 1000) -> List[Dict[str, Any]]:
        """Async wrapper for get_tasks"""
        return await self._run_in_executor(self.get_tasks_sync, craft, published, skip, limit)
    
    def delete_task_sync(self, name: str, craft: str) -> bool:
        """Delete task by name and craft"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"DELETE FROM {self._table('tasks')} WHERE name = %s AND craft = %s",
                (name, craft)
            )
            rows_deleted = cursor.rowcount
            conn.commit()
            return rows_deleted > 0
    
    async def delete_task(self, name: str, craft: str) -> bool:
        """Async wrapper for delete_task"""
        return await self._run_in_executor(self.delete_task_sync, name, craft)

    # ==================== USER PREFERENCES ====================

    def get_user_pref_sync(self, userid: int, name: str) -> Optional[str]:
        """Return the value of a Moodle user preference, or None if not set."""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            cursor.execute(
                f"SELECT value FROM {self.table_prefix}user_preferences WHERE userid = %s AND name = %s LIMIT 1",
                (userid, name),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return row['value'] if isinstance(row, dict) else row[0]

    async def get_user_pref(self, userid: int, name: str) -> Optional[str]:
        return await self._run_in_executor(self.get_user_pref_sync, userid, name)

    def set_user_pref_sync(self, userid: int, name: str, value: str) -> None:
        """Insert or update a Moodle user preference."""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor if DB_TYPE == 'postgresql' else None)
            if DB_TYPE == 'postgresql':
                cursor.execute(
                    f"""INSERT INTO {self.table_prefix}user_preferences (userid, name, value)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (userid, name) DO UPDATE SET value = EXCLUDED.value""",
                    (userid, name, value),
                )
            else:
                cursor.execute(
                    f"""INSERT INTO {self.table_prefix}user_preferences (userid, name, value)
                        VALUES (%s, %s, %s)
                        ON DUPLICATE KEY UPDATE value = VALUES(value)""",
                    (userid, name, value),
                )
            conn.commit()

    async def set_user_pref(self, userid: int, name: str, value: str) -> None:
        return await self._run_in_executor(self.set_user_pref_sync, userid, name, value)


# Alias for backwards compatibility and test imports
MoodleDB = MoodleDBAdapter

# Singleton instance
moodle_db = MoodleDBAdapter()

import aiosqlite
import json
import logging
from pathlib import Path
from ..config import DB_PATH

logger = logging.getLogger("mt.db")

class Database:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self._connection: aiosqlite.Connection | None = None

    async def connect(self):
        """Establish connection and apply initial setup."""
        if self._connection is None:
            self._connection = await aiosqlite.connect(self.db_path)
            # Enable WAL mode for concurrent access and foreign keys
            await self._connection.execute("PRAGMA journal_mode=WAL;")
            await self._connection.execute("PRAGMA foreign_keys=ON;")
            await self._connection.commit()
            await self._init_schema()
            logger.info(f"Database connected at {self.db_path} (WAL enabled)")

    async def disconnect(self):
        """Close database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None
            logger.info("Database disconnected")

    async def _init_schema(self):
        """Create tables if they do not exist."""
        # Media Library
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS media (
                id              TEXT PRIMARY KEY,
                file_name       TEXT NOT NULL,
                file_path       TEXT NOT NULL UNIQUE,
                file_size       INTEGER NOT NULL,
                sha256          TEXT,
                title           TEXT,
                description     TEXT,
                duration        REAL,
                width           INTEGER,
                height          INTEGER,
                fps             REAL,
                video_codec     TEXT,
                audio_codec     TEXT,
                container       TEXT,
                bitrate         INTEGER,
                youtube_id      TEXT,
                youtube_url     TEXT,
                uploader        TEXT,
                upload_date     TEXT,
                thumbnail_path  TEXT,
                source          TEXT NOT NULL DEFAULT 'download',
                source_peer_id  TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
                file_mtime      REAL
            );
        """)

        # Peers
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS peers (
                id              TEXT PRIMARY KEY,
                device_name     TEXT,
                addresses       TEXT, -- JSON list of IP:port strings
                protocol_version TEXT,
                library_count   INTEGER,
                library_size    INTEGER,
                capabilities    TEXT,
                first_seen      TEXT NOT NULL DEFAULT (datetime('now')),
                last_seen       TEXT NOT NULL DEFAULT (datetime('now')),
                is_online       INTEGER NOT NULL DEFAULT 1
            );
        """)

        # Transfers (P2P Send/Receive)
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS transfers (
                id              TEXT PRIMARY KEY,
                file_id         TEXT NOT NULL,
                peer_id         TEXT NOT NULL,
                direction       TEXT NOT NULL,
                file_name       TEXT NOT NULL,
                file_size       INTEGER NOT NULL,
                status          TEXT NOT NULL DEFAULT 'queued',
                progress        REAL NOT NULL DEFAULT 0.0,
                chunks_total    INTEGER,
                chunks_done     INTEGER DEFAULT 0,
                speed_bps       INTEGER,
                error_message   TEXT,
                started_at      TEXT,
                completed_at    TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );
        """)

        # Downloads (YouTube jobs)
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS downloads (
                id              TEXT PRIMARY KEY,
                url             TEXT NOT NULL,
                title           TEXT,
                status          TEXT NOT NULL DEFAULT 'queued',
                progress        REAL NOT NULL DEFAULT 0.0,
                speed_bps       INTEGER,
                eta_seconds     REAL,
                format_id       TEXT,
                media_id        TEXT REFERENCES media(id),
                error_message   TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                completed_at    TEXT
            );
        """)

        await self._connection.commit()
        logger.debug("Schema checks completed.")

    # Media-related operations
    async def add_media_item(self, item_dict: dict) -> str:
        """Insert or replace a media catalog entry."""
        keys = list(item_dict.keys())
        values = [item_dict[k] for k in keys]
        placeholders = ", ".join(["?"] * len(keys))
        columns = ", ".join(keys)
        
        query = f"INSERT OR REPLACE INTO media ({columns}) VALUES ({placeholders})"
        async with self._connection.execute(query, values) as cursor:
            await self._connection.commit()
            return item_dict.get("id")

    async def get_media_item(self, media_id: str) -> dict | None:
        self._connection.row_factory = aiosqlite.Row
        async with self._connection.execute("SELECT * FROM media WHERE id = ?", (media_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_media_by_path(self, filepath: str) -> dict | None:
        self._connection.row_factory = aiosqlite.Row
        async with self._connection.execute("SELECT * FROM media WHERE file_path = ?", (filepath,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_all_media(self) -> list[dict]:
        self._connection.row_factory = aiosqlite.Row
        async with self._connection.execute("SELECT * FROM media ORDER BY created_at DESC") as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def delete_media_item(self, media_id: str):
        await self._connection.execute("DELETE FROM media WHERE id = ?", (media_id,))
        await self._connection.commit()

    # Peer-related operations
    async def update_peer(self, peer_id: str, info_dict: dict):
        """Update or register discovered peers."""
        # Convert addresses list to JSON string if it is a list
        if "addresses" in info_dict and isinstance(info_dict["addresses"], list):
            info_dict["addresses"] = json.dumps(info_dict["addresses"])
            
        columns = list(info_dict.keys())
        values = [info_dict[c] for c in columns]
        
        # Build upsert query
        update_pairs = ", ".join([f"{c} = ?" for c in columns])
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))
        
        query = f"""
            INSERT INTO peers (id, {columns_str}) 
            VALUES (?, {placeholders})
            ON CONFLICT(id) DO UPDATE SET {update_pairs}, last_seen = datetime('now'), is_online = 1
        """
        async with self._connection.execute(query, [peer_id] + values + values) as cursor:
            await self._connection.commit()

    async def set_peer_offline(self, peer_id: str):
        await self._connection.execute("UPDATE peers SET is_online = 0 WHERE id = ?", (peer_id,))
        await self._connection.commit()

    async def get_online_peers(self) -> list[dict]:
        self._connection.row_factory = aiosqlite.Row
        async with self._connection.execute("SELECT * FROM peers WHERE is_online = 1") as cursor:
            rows = await cursor.fetchall()
            results = []
            for r in rows:
                d = dict(r)
                if d["addresses"]:
                    try:
                        d["addresses"] = json.loads(d["addresses"])
                    except json.JSONDecodeError:
                        pass
                results.append(d)
            return results

    # Download Jobs operations
    async def create_download_job(self, download_id: str, url: str) -> str:
        await self._connection.execute(
            "INSERT INTO downloads (id, url, status) VALUES (?, ?, 'queued')",
            (download_id, url)
        )
        await self._connection.commit()
        return download_id

    async def update_download_job(self, download_id: str, update_dict: dict):
        columns = list(update_dict.keys())
        values = [update_dict[c] for c in columns]
        set_clause = ", ".join([f"{c} = ?" for c in columns])
        
        query = f"UPDATE downloads SET {set_clause} WHERE id = ?"
        await self._connection.execute(query, values + [download_id])
        await self._connection.commit()

    async def get_download_job(self, download_id: str) -> dict | None:
        self._connection.row_factory = aiosqlite.Row
        async with self._connection.execute("SELECT * FROM downloads WHERE id = ?", (download_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_active_downloads(self) -> list[dict]:
        self._connection.row_factory = aiosqlite.Row
        async with self._connection.execute("SELECT * FROM downloads WHERE status NOT IN ('completed', 'failed')") as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]
            
    async def get_all_downloads(self) -> list[dict]:
        self._connection.row_factory = aiosqlite.Row
        async with self._connection.execute("SELECT * FROM downloads ORDER BY created_at DESC") as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

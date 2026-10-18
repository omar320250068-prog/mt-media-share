import asyncio
import os
import uuid
import logging
from pathlib import Path
from typing import Callable, Coroutine
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from ..config import DOWNLOADS_DIR
from ..db.database import Database
from ..utils.hashing import get_file_fast_identity, compute_file_hash
from .media_info import probe_media_file

logger = logging.getLogger("mt.scanner")

class LibraryScanner:
    def __init__(self, db: Database, event_callback: Callable[[dict], Coroutine] = None):
        self.db = db
        self.event_callback = event_callback
        self.watch_dir = DOWNLOADS_DIR
        self._observer: Observer | None = None
        self.loop = asyncio.get_running_loop()

    async def start(self):
        """Initiate startup scan and launch directory watchdog."""
        logger.info(f"Starting library scanner watching: {self.watch_dir}")
        
        # 1. Sync existing database files with actual files on disk
        await self.sync_library()
        
        # 2. Setup watcher
        event_handler = ScannerWatchdogHandler(self)
        self._observer = Observer()
        self._observer.schedule(event_handler, str(self.watch_dir), recursive=False)
        self._observer.start()
        logger.info("Library scanner directory watchdog started")

    async def stop(self):
        """Stop directory watchdog."""
        if self._observer:
            self._observer.stop()
            self._observer.join()
            self._observer = None
            logger.info("Library scanner directory watchdog stopped")

    async def sync_library(self):
        """Perform a full synchronization of database library records with files on disk."""
        logger.info("Performing full library sync...")
        
        # Gather all valid media files from downloads directory
        valid_extensions = {".mp4", ".mkv", ".webm", ".mp3", ".m4a"}
        disk_files: dict[str, Path] = {}
        for entry in self.watch_dir.iterdir():
            if entry.is_file() and entry.suffix.lower() in valid_extensions:
                disk_files[str(entry.resolve())] = entry
                
        # Gather all library records from database
        db_items = await self.db.get_all_media()
        db_paths = {item["file_path"]: item for item in db_items}
        
        # Delete entries from database that no longer exist on disk
        for db_path, item in db_paths.items():
            if db_path not in disk_files:
                logger.info(f"Removing media record for missing file: {item['file_name']}")
                await self.db.delete_media_item(item["id"])
                if self.event_callback:
                    await self.event_callback({
                        "event": "media_removed",
                        "data": {"id": item["id"]}
                    })

        # Register files on disk that are not yet in database
        for disk_path, path_obj in disk_files.items():
            if disk_path not in db_paths:
                await self.register_file(path_obj)
            else:
                # Fast check if file has changed (mtime or size modified)
                db_item = db_paths[disk_path]
                try:
                    mtime, size = get_file_fast_identity(path_obj)
                    if db_item["file_mtime"] != mtime or db_item["file_size"] != size:
                        logger.info(f"File change detected for {path_obj.name}, re-registering.")
                        await self.register_file(path_obj)
                except Exception as e:
                    logger.warning(f"Could not verify fast change check for {path_obj.name}: {e}")

        logger.info("Library sync completed.")

    async def register_file(self, path: Path):
        """Analyze, hash, extract metadata, and insert file into database."""
        logger.info(f"Registering new media file: {path.name}")
        try:
            # Prevent cataloging temporary download files from yt-dlp
            if path.suffix.lower() == ".part" or path.name.endswith(".ytdl"):
                return

            mtime, size = get_file_fast_identity(path)
            
            # Extract basic media details
            probe = probe_media_file(path)
            
            # Setup database record
            media_id = str(uuid.uuid4())
            media_item = {
                "id": media_id,
                "file_name": path.name,
                "file_path": str(path.resolve()),
                "file_size": size,
                "title": path.stem,
                "description": "",
                "duration": probe.get("duration", 0),
                "width": probe.get("width"),
                "height": probe.get("height"),
                "fps": probe.get("fps"),
                "video_codec": probe.get("video_codec"),
                "audio_codec": probe.get("audio_codec"),
                "container": probe.get("container"),
                "bitrate": probe.get("bitrate", 0),
                "source": "manual",  # If sync found it, it's manually added
                "file_mtime": mtime
            }
            
            # Store in DB
            await self.db.add_media_item(media_item)
            
            # Notify UI
            if self.event_callback:
                await self.event_callback({
                    "event": "media_added",
                    "data": media_item
                })
                
            # Compute full SHA-256 hash in a background thread to prevent thread blocks
            asyncio.create_task(self._calculate_hash_async(media_id, path))
            
        except Exception as e:
            logger.error(f"Error registering file {path.name}: {e}", exc_info=True)

    async def _calculate_hash_async(self, media_id: str, path: Path):
        """Asynchronously compute and store SHA-256 for registered media items."""
        try:
            # Use run_in_executor since streaming hash is blocking CPU work
            sha256 = await self.loop.run_in_executor(None, compute_file_hash, path)
            
            # Update DB entry
            item = await self.db.get_media_item(media_id)
            if item:
                item["sha256"] = sha256
                await self.db.add_media_item(item)
                logger.info(f"Computed SHA-256 hash for {path.name}: {sha256}")
                
                # Notify UI of catalog updates
                if self.event_callback:
                    await self.event_callback({
                        "event": "media_updated",
                        "data": {"id": media_id, "sha256": sha256}
                    })
        except Exception as e:
            logger.error(f"Error hashing file {path.name}: {e}")

    async def handle_fs_event(self, event_type: str, src_path: str):
        """Route file events correctly into the async loop."""
        path = Path(src_path)
        valid_extensions = {".mp4", ".mkv", ".webm", ".mp3", ".m4a"}
        
        if path.suffix.lower() not in valid_extensions:
            return
            
        logger.debug(f"Filesystem event [{event_type}] for {path.name}")
        
        if event_type == "created" or event_type == "modified":
            await self.register_file(path)
        elif event_type == "deleted":
            db_item = await self.db.get_media_by_path(str(path.resolve()))
            if db_item:
                logger.info(f"Deleted file detected on disk: {path.name}. Removing database record.")
                await self.db.delete_media_item(db_item["id"])
                if self.event_callback:
                    await self.event_callback({
                        "event": "media_removed",
                        "data": {"id": db_item["id"]}
                    })

class ScannerWatchdogHandler(FileSystemEventHandler):
    def __init__(self, scanner: LibraryScanner):
        self.scanner = scanner

    def on_created(self, event: FileSystemEvent):
        if not event.is_directory:
            asyncio.run_coroutine_threadsafe(
                self.scanner.handle_fs_event("created", event.src_path),
                self.scanner.loop
            )

    def on_modified(self, event: FileSystemEvent):
        if not event.is_directory:
            asyncio.run_coroutine_threadsafe(
                self.scanner.handle_fs_event("modified", event.src_path),
                self.scanner.loop
            )

    def on_deleted(self, event: FileSystemEvent):
        if not event.is_directory:
            asyncio.run_coroutine_threadsafe(
                self.scanner.handle_fs_event("deleted", event.src_path),
                self.scanner.loop
            )

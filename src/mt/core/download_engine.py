import asyncio
import uuid
import logging
import yt_dlp
from pathlib import Path
from typing import Callable, Coroutine
from ..config import DOWNLOADS_DIR, get_ffmpeg_path
from ..db.database import Database

logger = logging.getLogger("mt.downloader")

class DownloadEngine:
    def __init__(self, db: Database, event_callback: Callable[[dict], Coroutine] = None):
        self.db = db
        self.event_callback = event_callback
        self.loop = asyncio.get_running_loop()
        self.ffmpeg_path = get_ffmpeg_path()
        self._active_downloads: dict[str, asyncio.Future] = {}

    def _get_ytdl_opts(self, job_id: str, format_preset: str, dest_dir: Path) -> dict:
        """Construct options dictionary for yt-dlp."""
        
        # Quality presets
        if format_preset == "1080p":
            fmt = "bestvideo[height<=1080]+bestaudio/best[height<=1080]"
        elif format_preset == "720p":
            fmt = "bestvideo[height<=720]+bestaudio/best[height<=720]"
        elif format_preset == "audio":
            fmt = "bestaudio/best"
        else:
            # Maximum quality
            fmt = "bestvideo+bestaudio/best"

        outtmpl = str(dest_dir / "%(title)s.%(ext)s")

        opts = {
            "format": fmt,
            "outtmpl": outtmpl,
            "noplaylist": True,
            "progress_hooks": [lambda d: self._progress_hook(job_id, d)],
            "postprocessor_hooks": [lambda d: self._postprocessor_hook(job_id, d)],
            # Keep log cleaner but record warnings/errors
            "quiet": True,
            "no_warnings": False,
        }

        # Inject postprocessing configuration
        postprocessors = []

        if format_preset == "audio":
            postprocessors.append({
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            })
        else:
            opts["merge_output_format"] = "mp4"
            postprocessors.append({
                "key": "FFmpegMetadata",
                "add_chapters": True,
                "add_metadata": True,
            })
            postprocessors.append({
                "key": "EmbedThumbnail",
            })
            opts["writethumbnail"] = True

        opts["postprocessors"] = postprocessors

        if self.ffmpeg_path:
            opts["ffmpeg_location"] = self.ffmpeg_path

        return opts

    def _progress_hook(self, job_id: str, d: dict):
        """yt-dlp download progress callback. Note: executes inside the thread pool executor thread."""
        status = d.get("status")
        
        if status == "downloading":
            downloaded = d.get("downloaded_bytes", 0)
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            progress = downloaded / total if total > 0 else 0.0
            
            speed = d.get("speed", 0) or 0
            eta = d.get("eta", 0) or 0
            
            # Fire database and event updates asynchronously in the main loop
            asyncio.run_coroutine_threadsafe(
                self._update_job_status(job_id, {
                    "status": "downloading",
                    "progress": progress,
                    "speed_bps": int(speed),
                    "eta_seconds": float(eta),
                    "title": Path(d.get("filename", "")).stem
                }),
                self.loop
            )
        elif status == "finished":
            asyncio.run_coroutine_threadsafe(
                self._update_job_status(job_id, {
                    "status": "processing",
                    "progress": 1.0,
                    "speed_bps": 0,
                    "eta_seconds": 0
                }),
                self.loop
            )
        elif status == "error":
            asyncio.run_coroutine_threadsafe(
                self._update_job_status(job_id, {
                    "status": "failed",
                    "error_message": "An error occurred during video download."
                }),
                self.loop
            )

    def _postprocessor_hook(self, job_id: str, d: dict):
        """yt-dlp post-processing callback."""
        status = d.get("status")
        pp_name = d.get("postprocessor")
        
        if status == "started":
            asyncio.run_coroutine_threadsafe(
                self._update_job_status(job_id, {
                    "status": "processing",
                    "error_message": f"Processing with {pp_name}..."
                }),
                self.loop
            )
        elif status == "finished":
            logger.info(f"Postprocessor {pp_name} completed for job {job_id}")

    async def _update_job_status(self, job_id: str, data: dict):
        """Helper to write status updates to database and dispatch WebSocket signals."""
        await self.db.update_download_job(job_id, data)
        if self.event_callback:
            # Emit websocket updates to the UI layer
            await self.event_callback({
                "event": "download_progress",
                "data": {"id": job_id, **data}
            })

    def _run_ytdl_download(self, url: str, opts: dict) -> dict:
        """Run the actual blocking download inside the executor thread."""
        with yt_dlp.YoutubeDL(opts) as ydl:
            # Extract info to get metadata, then download
            info = ydl.extract_info(url, download=True)
            return ydl.sanitize_info(info)

    async def download(self, url: str, quality: str = "best") -> str:
        """Initiate non-blocking background download."""
        job_id = str(uuid.uuid4())
        await self.db.create_download_job(job_id, url)
        
        # Setup run task in worker thread
        opts = self._get_ytdl_opts(job_id, quality, DOWNLOADS_DIR)
        
        async def task_wrapper():
            try:
                logger.info(f"Starting download for {url} (job: {job_id})")
                await self._update_job_status(job_id, {"status": "extracting", "progress": 0.05})
                
                # Offload blocking call to the default ThreadPoolExecutor
                info = await self.loop.run_in_executor(None, self._run_ytdl_download, url, opts)
                
                # Resolve final file path (if merged, yt-dlp lists it under info['requested_downloads'])
                file_path = info.get("_filename")
                if "requested_downloads" in info and info["requested_downloads"]:
                    file_path = info["requested_downloads"][0].get("filepath", file_path)
                
                file_path_obj = Path(file_path)
                
                # Get actual file stats
                file_size = file_path_obj.stat().st_size if file_path_obj.exists() else 0
                
                # Check for cached thumbnail file created by EmbedThumbnail
                thumb_path = ""
                # Default thumbnail image downloaded by yt-dlp is usually .jpg/.png
                thumbnail_candidates = list(DOWNLOADS_DIR.glob(f"{file_path_obj.stem}.*"))
                for tc in thumbnail_candidates:
                    if tc.suffix.lower() in [".jpg", ".png", ".webp", ".jpeg"] and tc != file_path_obj:
                        thumb_path = str(tc)
                        break

                # Create database entry in media table
                media_id = str(uuid.uuid4())
                media_item = {
                    "id": media_id,
                    "file_name": file_path_obj.name,
                    "file_path": str(file_path_obj.resolve()),
                    "file_size": file_size,
                    "title": info.get("title", file_path_obj.stem),
                    "description": info.get("description", ""),
                    "duration": info.get("duration", 0),
                    "width": info.get("width"),
                    "height": info.get("height"),
                    "fps": info.get("fps"),
                    "uploader": info.get("uploader", ""),
                    "upload_date": info.get("upload_date", ""),
                    "youtube_id": info.get("id"),
                    "youtube_url": url,
                    "container": file_path_obj.suffix.lstrip("."),
                    "source": "download",
                    "thumbnail_path": thumb_path,
                    "file_mtime": file_path_obj.stat().st_mtime
                }
                
                await self.db.add_media_item(media_item)
                
                # Mark job as completed
                await self._update_job_status(job_id, {
                    "status": "completed",
                    "progress": 1.0,
                    "media_id": media_id,
                    "completed_at": asyncio.get_event_loop().time()
                })
                
                # Notify media library change
                if self.event_callback:
                    await self.event_callback({
                        "event": "media_added",
                        "data": media_item
                    })

                logger.info(f"Finished download job {job_id} successfully. File: {file_path_obj.name}")
            except Exception as e:
                logger.error(f"Download job {job_id} failed: {e}", exc_info=True)
                await self._update_job_status(job_id, {
                    "status": "failed",
                    "error_message": str(e)
                })
            finally:
                self._active_downloads.pop(job_id, None)

        # Start execution
        self._active_downloads[job_id] = asyncio.create_task(task_wrapper())
        return job_id

    def cancel_download(self, job_id: str) -> bool:
        """Cancel a running download task."""
        task = self._active_downloads.get(job_id)
        if task and not task.done():
            task.cancel()
            logger.info(f"Cancelled download job {job_id}")
            asyncio.create_task(self._update_job_status(job_id, {
                "status": "failed",
                "error_message": "Cancelled by user"
            }))
            return True
        return False

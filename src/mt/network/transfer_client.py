import asyncio
import time
import uuid
import logging
from pathlib import Path
from typing import Callable, Coroutine
from ..constants import (
    MSG_CATALOG_REQ, MSG_CATALOG_RES,
    MSG_TRANSFER_REQ, MSG_TRANSFER_META,
    MSG_CHUNK_HEADER, MSG_CHUNK_DATA,
    MSG_CHUNK_ACK, MSG_TRANSFER_COMPLETE,
    MSG_ERROR
)
from ..db.database import Database
from .protocol import read_packet, create_packet, parse_json_payload
from .manifest import create_manifest_file, read_manifest, write_manifest, get_next_missing_chunk
from ..utils.hashing import compute_chunk_hash, compute_file_hash
from ..config import DOWNLOADS_DIR
from ..core.media_info import probe_media_file

logger = logging.getLogger("mt.network.client")

class TransferClient:
    def __init__(self, db: Database, event_callback: Callable[[dict], Coroutine] = None):
        self.db = db
        self.event_callback = event_callback
        self.loop = asyncio.get_running_loop()
        self._active_downloads: dict[str, asyncio.Task] = {}

    async def fetch_peer_catalog(self, peer_ip: str, peer_port: int) -> list[dict]:
        """Fetch remote media catalog listing from peer."""
        logger.info(f"Fetching library catalog from peer at {peer_ip}:{peer_port}")
        reader, writer = await asyncio.open_connection(peer_ip, peer_port)
        try:
            writer.write(create_packet(MSG_CATALOG_REQ, {}))
            await writer.drain()
            
            msg_type, payload = await read_packet(reader)
            if msg_type == MSG_CATALOG_RES:
                return parse_json_payload(payload)
            elif msg_type == MSG_ERROR:
                err = parse_json_payload(payload)
                raise RuntimeError(f"Peer returned error: {err.get('message')}")
            else:
                raise RuntimeError(f"Unexpected packet response type {msg_type}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def start_transfer(self, peer_id: str, peer_ip: str, peer_port: int, file_id: str, file_name: str) -> str:
        """Queue and run file transfer download from peer."""
        transfer_id = str(uuid.uuid4())
        
        # Start background downloader task
        task = asyncio.create_task(
            self._download_job_task(transfer_id, peer_id, peer_ip, peer_port, file_id, file_name)
        )
        self._active_downloads[transfer_id] = task
        return transfer_id

    async def _download_job_task(self, t_id: str, peer_id: str, ip: str, port: int, file_id: str, file_name: str):
        # Create transfer db record
        await self.db.update_peer(peer_id, {"is_online": 1})  # Make sure peer exists
        
        # Initial database record
        t_record = {
            "id": t_id,
            "file_id": file_id,
            "peer_id": peer_id,
            "direction": "incoming",
            "file_name": file_name,
            "file_size": 0,
            "status": "queued",
            "progress": 0.0,
            "chunks_total": 0,
            "chunks_done": 0,
            "speed_bps": 0,
            "started_at": None,
            "completed_at": None
        }
        
        # SQLite raw insertion helper
        query = """
            INSERT INTO transfers (id, file_id, peer_id, direction, file_name, file_size, status, progress, chunks_total, chunks_done)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        async with self.db._connection.execute(query, (t_id, file_id, peer_id, "incoming", file_name, 0, "queued", 0.0, 0, 0)) as cursor:
            await self.db._connection.commit()

        # Define file target paths
        dest_path = DOWNLOADS_DIR / file_name
        part_path = DOWNLOADS_DIR / f"{file_name}.part"
        manifest_path = DOWNLOADS_DIR / f".{file_name}.mt_manifest"

        logger.info(f"Connecting to peer for transfer {file_name}")
        
        writer = None
        try:
            # Connect to peer
            reader, writer = await asyncio.open_connection(ip, port)
            
            # Setup or restore manifest
            manifest = read_manifest(manifest_path)
            if manifest and manifest["file_id"] == file_id:
                resume_from = get_next_missing_chunk(manifest)
                logger.info(f"Resuming file transfer from chunk index {resume_from}")
            else:
                # Fresh transfer setup
                manifest = None
                resume_from = 0

            # Update DB state
            t_record["status"] = "active"
            t_record["started_at"] = str(time.time())
            await self._update_transfer_record(t_id, {"status": "active", "started_at": t_record["started_at"]})

            # Send file request command
            req = {"file_id": file_id, "resume_from_chunk": resume_from}
            writer.write(create_packet(MSG_TRANSFER_REQ, req))
            await writer.drain()

            # Read meta response
            msg_type, payload = await read_packet(reader)
            if msg_type == MSG_ERROR:
                err = parse_json_payload(payload)
                raise RuntimeError(f"Peer rejected request: {err.get('message')}")
            elif msg_type != MSG_TRANSFER_META:
                raise RuntimeError(f"Unexpected packet response {msg_type} (expected transfer metadata)")

            meta = parse_json_payload(payload)
            file_size = meta["file_size"]
            total_chunks = meta["total_chunks"]
            chunk_size = meta["chunk_size"]
            file_sha256 = meta["sha256"]

            # Initialize manifest if new
            if not manifest:
                manifest = create_manifest_file(
                    manifest_path,
                    file_id=file_id,
                    file_name=file_name,
                    file_size=file_size,
                    file_sha256=file_sha256,
                    chunk_size=chunk_size,
                    source_peer_id=peer_id,
                    started_at=str(time.time()),
                    partial_file_path=str(part_path.resolve())
                )

            # Update database metrics
            t_record["file_size"] = file_size
            t_record["chunks_total"] = total_chunks
            t_record["chunks_done"] = resume_from
            await self._update_transfer_record(t_id, {
                "file_size": file_size,
                "chunks_total": total_chunks,
                "chunks_done": resume_from
            })

            # Open file stream and seek
            open_mode = "r+b" if part_path.exists() and resume_from > 0 else "wb"
            
            # Transfer loops
            bytes_received = resume_from * chunk_size
            start_time = time.time()
            last_db_update = start_time
            last_bytes_received = bytes_received

            with open(part_path, open_mode) as f:
                if resume_from > 0:
                    f.seek(resume_from * chunk_size)
                
                for i in range(resume_from, total_chunks):
                    # 1. Read header packet
                    msg_type, payload = await read_packet(reader)
                    if msg_type == MSG_ERROR:
                        err = parse_json_payload(payload)
                        raise RuntimeError(f"Peer error during transfer: {err.get('message')}")
                    elif msg_type != MSG_CHUNK_HEADER:
                        raise RuntimeError(f"Malformed stream header packet: {msg_type}")

                    chunk_header = parse_json_payload(payload)
                    chunk_index = chunk_header["chunk_index"]
                    expected_chunk_hash = chunk_header["sha256"]
                    expected_chunk_size = chunk_header["chunk_size"]

                    # 2. Read raw binary chunk data packet
                    msg_type, chunk_data = await read_packet(reader)
                    if msg_type != MSG_CHUNK_DATA:
                        raise RuntimeError(f"Malformed stream payload packet: {msg_type}")

                    # Validate integrity of received block bytes
                    chunk_hash = compute_chunk_hash(chunk_data)
                    if chunk_hash != expected_chunk_hash:
                        logger.error(f"Chunk integrity failure at index {chunk_index}!")
                        writer.write(create_packet(MSG_CHUNK_ACK, {"chunk_index": chunk_index, "status": "retry"}))
                        await writer.drain()
                        raise ValueError(f"Integrity check failed at chunk {chunk_index}")

                    # Write block to correct position in file
                    f.write(chunk_data)
                    bytes_received += len(chunk_data)

                    # Acknowledge correct chunk packet
                    writer.write(create_packet(MSG_CHUNK_ACK, {"chunk_index": chunk_index, "status": "ok"}))
                    await writer.drain()

                    # Save manifest updates
                    manifest["completed_chunks"][str(chunk_index)] = chunk_hash
                    # Save manifest to disk every 10 blocks or on last block
                    if chunk_index % 10 == 0 or chunk_index == total_chunks - 1:
                        manifest["last_updated"] = str(time.time())
                        write_manifest(manifest_path, manifest)

                    # Track speed and update progress periodically
                    now = time.time()
                    if now - last_db_update >= 0.5 or chunk_index == total_chunks - 1:
                        elapsed = now - last_db_update
                        delta_bytes = bytes_received - last_bytes_received
                        speed = int(delta_bytes / elapsed) if elapsed > 0 else 0
                        
                        progress = bytes_received / file_size if file_size > 0 else 0.0
                        
                        await self._update_transfer_record(t_id, {
                            "progress": progress,
                            "chunks_done": chunk_index + 1,
                            "speed_bps": speed
                        })
                        
                        last_db_update = now
                        last_bytes_received = bytes_received

            # 4. Stream ended. Verify final SHA-256 integrity
            logger.info("Computing final hash check validation...")
            # Compute full SHA-256 hash in a background thread to prevent thread blocks
            actual_file_sha256 = await self.loop.run_in_executor(None, compute_file_hash, part_path)
            
            if actual_file_sha256 != file_sha256:
                writer.write(create_packet(MSG_TRANSFER_COMPLETE, {"file_sha256_verified": False}))
                await writer.drain()
                raise ValueError("Complete file integrity validation failed after stream assembly!")

            # Complete handshake
            writer.write(create_packet(MSG_TRANSFER_COMPLETE, {"file_sha256_verified": True}))
            await writer.drain()

            # Clean and finalize file paths
            if dest_path.exists():
                dest_path.unlink()
            part_path.rename(dest_path)
            if manifest_path.exists():
                manifest_path.unlink()

            # Analyze file details using probe
            probe = probe_media_file(dest_path)

            # Save file record in database
            media_id = str(uuid.uuid4())
            media_item = {
                "id": media_id,
                "file_name": dest_path.name,
                "file_path": str(dest_path.resolve()),
                "file_size": file_size,
                "sha256": file_sha256,
                "title": dest_path.stem,
                "description": "",
                "duration": probe.get("duration", 0),
                "width": probe.get("width"),
                "height": probe.get("height"),
                "fps": probe.get("fps"),
                "video_codec": probe.get("video_codec"),
                "audio_codec": probe.get("audio_codec"),
                "container": probe.get("container"),
                "bitrate": probe.get("bitrate", 0),
                "source": "p2p",
                "source_peer_id": peer_id,
                "file_mtime": dest_path.stat().st_mtime
            }
            await self.db.add_media_item(media_item)

            # Update transfer catalog to completed status
            await self._update_transfer_record(t_id, {
                "status": "completed",
                "progress": 1.0,
                "completed_at": str(time.time()),
                "speed_bps": 0
            })
            
            if self.event_callback:
                await self.event_callback({
                    "event": "media_added",
                    "data": media_item
                })

            logger.info(f"P2P transfer download of {file_name} completed and verified successfully!")

        except Exception as e:
            logger.error(f"P2P transfer job failed: {e}", exc_info=True)
            await self._update_transfer_record(t_id, {
                "status": "failed",
                "error_message": str(e),
                "speed_bps": 0
            })
        finally:
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
            self._active_downloads.pop(t_id, None)

    async def _update_transfer_record(self, t_id: str, update_dict: dict):
        """Update transfer record in SQLite and notify event callback."""
        columns = list(update_dict.keys())
        values = [update_dict[c] for c in columns]
        set_clause = ", ".join([f"{c} = ?" for c in columns])
        
        query = f"UPDATE transfers SET {set_clause} WHERE id = ?"
        async with self.db._connection.execute(query, values + [t_id]) as cursor:
            await self.db._connection.commit()

        if self.event_callback:
            await self.event_callback({
                "event": "transfer_progress",
                "data": {"id": t_id, **update_dict}
            })

    def cancel_transfer(self, t_id: str) -> bool:
        task = self._active_downloads.get(t_id)
        if task and not task.done():
            task.cancel()
            logger.info(f"Cancelled file transfer {t_id}")
            asyncio.create_task(self._update_transfer_record(t_id, {
                "status": "failed",
                "error_message": "Cancelled by user"
            }))
            return True
        return False

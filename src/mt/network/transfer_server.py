import asyncio
import os
import logging
from pathlib import Path
from ..constants import (
    P2P_PORT, CHUNK_SIZE,
    MSG_CATALOG_REQ, MSG_CATALOG_RES,
    MSG_TRANSFER_REQ, MSG_TRANSFER_META,
    MSG_CHUNK_HEADER, MSG_CHUNK_DATA,
    MSG_CHUNK_ACK, MSG_TRANSFER_COMPLETE,
    MSG_ERROR
)
from ..db.database import Database
from .protocol import read_packet, create_packet, parse_json_payload
from ..utils.hashing import compute_chunk_hash

logger = logging.getLogger("mt.network.server")

class TransferServer:
    def __init__(self, db: Database):
        self.db = db
        self.server: asyncio.Server | None = None
        self._active_connections: set[asyncio.Task] = set()

    async def start(self, port: int = P2P_PORT):
        """Start the P2P transfer listener server."""
        self.server = await asyncio.start_server(self._handle_client, "0.0.0.0", port)
        logger.info(f"P2P Transfer Server listening on 0.0.0.0:{port}")
        
        # Start connection tracking cleanup loop
        asyncio.create_task(self._monitor_server())

    async def stop(self):
        """Shut down the server and close active connection tasks."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
            logger.info("P2P Transfer Server stopped")
            
        # Cancel active transfer routines
        for task in list(self._active_connections):
            task.cancel()
        if self._active_connections:
            await asyncio.gather(*self._active_connections, return_exceptions=True)
            self._active_connections.clear()

    async def _monitor_server(self):
        while self.server is not None:
            # Clean completed connection tasks
            self._active_connections = {t for t in self._active_connections if not t.done()}
            await asyncio.sleep(5)

    def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Connection handler callback."""
        task = asyncio.create_task(self._process_session(reader, writer))
        self._active_connections.add(task)

    async def _process_session(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer_addr = writer.get_extra_info("peername")
        logger.info(f"Accepted TCP connection from {peer_addr}")
        
        try:
            while True:
                try:
                    msg_type, payload = await read_packet(reader)
                except (ConnectionResetError, asyncio.IncompleteReadError):
                    logger.debug(f"Peer {peer_addr} disconnected gracefully")
                    break

                if msg_type == MSG_CATALOG_REQ:
                    await self._handle_catalog_request(writer)
                elif msg_type == MSG_TRANSFER_REQ:
                    await self._handle_transfer_request(reader, writer, payload)
                    # A transfer request is a terminal exchange session; break after sending
                    break
                else:
                    logger.warning(f"Unknown command {msg_type} from peer {peer_addr}")
                    writer.write(create_packet(MSG_ERROR, {"message": "Invalid command"}))
                    await writer.drain()
                    break
        except asyncio.CancelledError:
            logger.debug(f"Connection session with {peer_addr} was cancelled")
        except Exception as e:
            logger.error(f"Error handling connection from {peer_addr}: {e}", exc_info=True)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.debug(f"Closed connection with {peer_addr}")

    async def _handle_catalog_request(self, writer: asyncio.StreamWriter):
        """Respond to catalog directory request."""
        logger.debug("Handling catalog list query")
        items = await self.db.get_all_media()
        
        # Strip internal absolute path and metadata to avoid leaks
        public_items = []
        for item in items:
            # Send only verified hashed media items
            if item.get("sha256"):
                public_items.append({
                    "id": item["id"],
                    "file_name": item["file_name"],
                    "file_size": item["file_size"],
                    "sha256": item["sha256"],
                    "title": item["title"],
                    "duration": item["duration"],
                    "container": item["container"]
                })

        writer.write(create_packet(MSG_CATALOG_RES, public_items))
        await writer.drain()
        logger.debug(f"Sent catalog of {len(public_items)} items")

    async def _handle_transfer_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, payload_bytes: bytes):
        """Handle request to download a file from client."""
        try:
            req = parse_json_payload(payload_bytes)
        except Exception:
            writer.write(create_packet(MSG_ERROR, {"message": "Malformed transfer query"}))
            await writer.drain()
            return

        file_id = req.get("file_id")
        resume_chunk = req.get("resume_from_chunk", 0)

        # 1. Fetch file from database catalog
        media = await self.db.get_media_item(file_id)
        if not media or not media.get("sha256"):
            logger.warning(f"Transfer request failed: file ID {file_id} not found in catalog")
            writer.write(create_packet(MSG_ERROR, {"message": "File not found"}))
            await writer.drain()
            return

        file_path = Path(media["file_path"])
        if not file_path.exists():
            logger.warning(f"File not found on disk at {file_path}")
            writer.write(create_packet(MSG_ERROR, {"message": "File deleted from disk"}))
            await writer.drain()
            return

        file_size = file_path.stat().st_size
        total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

        # 2. Transmit metadata details
        meta_payload = {
            "file_id": file_id,
            "file_name": media["file_name"],
            "file_size": file_size,
            "total_chunks": total_chunks,
            "chunk_size": CHUNK_SIZE,
            "sha256": media["sha256"]
        }
        writer.write(create_packet(MSG_TRANSFER_META, meta_payload))
        await writer.drain()

        # 3. Stream chunks sequentially
        logger.info(f"Streaming {media['file_name']} starting from chunk {resume_chunk}/{total_chunks}")
        
        try:
            with open(file_path, "rb") as f:
                f.seek(resume_chunk * CHUNK_SIZE)
                
                for i in range(resume_chunk, total_chunks):
                    chunk_data = f.read(CHUNK_SIZE)
                    if not chunk_data:
                        break
                    
                    chunk_hash = compute_chunk_hash(chunk_data)
                    
                    # Send chunk header
                    chunk_header = {
                        "chunk_index": i,
                        "chunk_size": len(chunk_data),
                        "sha256": chunk_hash
                    }
                    writer.write(create_packet(MSG_CHUNK_HEADER, chunk_header))
                    
                    # Send chunk payload
                    writer.write(create_packet(MSG_CHUNK_DATA, chunk_data))
                    await writer.drain()
                    
                    # Await ACK response
                    msg_type, ack_payload_bytes = await read_packet(reader)
                    if msg_type == MSG_CHUNK_ACK:
                        ack_data = parse_json_payload(ack_payload_bytes)
                        if ack_data.get("status") != "ok":
                            logger.error(f"Client reported error at chunk {i}, aborting transfer")
                            return
                    else:
                        logger.error(f"Unexpected packet {msg_type} during transfer, aborting")
                        return

            # Read completion status
            msg_type, status_payload = await read_packet(reader)
            if msg_type == MSG_TRANSFER_COMPLETE:
                status = parse_json_payload(status_payload)
                if status.get("file_sha256_verified"):
                    logger.info(f"File transfer of {media['file_name']} completed and verified successfully by receiver!")
                else:
                    logger.error(f"Receiver reported SHA-256 hash validation failure for {media['file_name']}")
        except Exception as e:
            logger.error(f"Exception encountered during streaming of {media['file_name']}: {e}")
            try:
                writer.write(create_packet(MSG_ERROR, {"message": "Server error during stream"}))
                await writer.drain()
            except Exception:
                pass

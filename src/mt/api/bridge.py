import asyncio
import json
import os
import logging
from pathlib import Path
from aiohttp import web
from ..config import API_PORT
from ..db.database import Database
from ..core.download_engine import DownloadEngine
from ..network.transfer_client import TransferClient
from ..network.discovery import DiscoveryManager

logger = logging.getLogger("mt.api.bridge")

class ApiBridge:
    def __init__(
        self,
        db: Database,
        downloader: DownloadEngine,
        client: TransferClient,
        discovery: DiscoveryManager
    ):
        self.db = db
        self.downloader = downloader
        self.client = client
        self.discovery = discovery
        self.app = web.Application()
        self.app.router.add_get("/ws", self._websocket_handler)
        self.runner: web.AppRunner | None = None
        self._sockets: set[web.WebSocketResponse] = set()

    async def start(self, port: int = API_PORT):
        """Start the API WebSocket server listener."""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", port)
        await site.start()
        logger.info(f"WebSocket API Bridge listening on ws://127.0.0.1:{port}/ws")

    async def stop(self):
        """Shut down the API server and drop client sockets."""
        for ws in list(self._sockets):
            await ws.close(code=1001, message="Server shutting down")
        self._sockets.clear()
        
        if self.runner:
            await self.runner.cleanup()
            self.runner = None
            logger.info("WebSocket API Bridge stopped")

    async def broadcast_event(self, event_data: dict):
        """Broadcast events (like download progress, peer discovered) to all active web clients."""
        payload = json.dumps(event_data)
        logger.debug(f"Broadcasting websocket event: {event_data.get('event')}")
        
        # Gather write tasks to run concurrently
        tasks = []
        for ws in list(self._sockets):
            if not ws.closed:
                tasks.append(ws.send_str(payload))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _websocket_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        self._sockets.add(ws)
        peername = request.transport.get_extra_info("peername")
        logger.info(f"UI WebSocket client connected from {peername}")
        
        try:
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        msg_id = data.get("id")
                        method = data.get("method")
                        params = data.get("params", {})
                        
                        logger.debug(f"WebSocket API request: method={method}")
                        
                        # Process RPC method
                        result = await self._dispatch_method(method, params)
                        
                        # Return success response
                        await ws.send_str(json.dumps({
                            "id": msg_id,
                            "result": result,
                            "error": None
                        }))
                    except Exception as e:
                        logger.error(f"Error handling WebSocket message: {e}", exc_info=True)
                        await ws.send_str(json.dumps({
                            "id": data.get("id") if "data" in locals() and isinstance(data, dict) else None,
                            "result": None,
                            "error": str(e)
                        }))
                elif msg.type == web.WSMsgType.ERROR:
                    logger.error(f"WebSocket connection closed with error: {ws.exception()}")
        finally:
            self._sockets.discard(ws)
            logger.info("UI WebSocket client disconnected")
        return ws

    async def _dispatch_method(self, method: str, params: dict) -> any:
        """Route JSON-RPC method request calls."""
        if method == "get_library":
            return await self.db.get_all_media()
            
        elif method == "get_downloads":
            return await self.db.get_all_downloads()
            
        elif method == "get_peers":
            return await self.db.get_online_peers()
            
        elif method == "get_transfers":
            self.db._connection.row_factory = aiosqlite.Row
            async with self.db._connection.execute("SELECT * FROM transfers ORDER BY created_at DESC") as cursor:
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

        elif method == "download_video":
            url = params.get("url")
            quality = params.get("quality", "best")
            if not url:
                raise ValueError("Parameters error: missing target download URL")
            job_id = await self.downloader.download(url, quality)
            return {"job_id": job_id}

        elif method == "cancel_download":
            job_id = params.get("job_id")
            success = self.downloader.cancel_download(job_id)
            return {"success": success}

        elif method == "fetch_peer_catalog":
            peer_ip = params.get("ip")
            peer_port = int(params.get("port", 8400))
            if not peer_ip:
                raise ValueError("Parameters error: missing peer IP address")
            catalog = await self.client.fetch_peer_catalog(peer_ip, peer_port)
            return catalog

        elif method == "download_peer_file":
            peer_id = params.get("peer_id")
            peer_ip = params.get("peer_ip")
            peer_port = int(params.get("peer_port", 8400))
            file_id = params.get("file_id")
            file_name = params.get("file_name")
            
            if not all([peer_id, peer_ip, file_id, file_name]):
                raise ValueError("Parameters error: missing transfer connection attributes")
                
            transfer_id = await self.client.start_transfer(peer_id, peer_ip, peer_port, file_id, file_name)
            return {"transfer_id": transfer_id}

        elif method == "cancel_transfer":
            transfer_id = params.get("transfer_id")
            success = self.client.cancel_transfer(transfer_id)
            return {"success": success}

        elif method == "delete_media":
            media_id = params.get("media_id")
            item = await self.db.get_media_item(media_id)
            if item:
                # Remove database entry
                await self.db.delete_media_item(media_id)
                # Attempt to delete file from disk
                file_path = Path(item["file_path"])
                if file_path.exists():
                    try:
                        file_path.unlink()
                        logger.info(f"Deleted media file from disk: {file_path.name}")
                    except Exception as e:
                        logger.warning(f"Could not delete file from disk {file_path.name}: {e}")
                
                # Delete thumbnail file if exists
                if item.get("thumbnail_path"):
                    thumb_path = Path(item["thumbnail_path"])
                    if thumb_path.exists():
                        try:
                            thumb_path.unlink()
                        except Exception:
                            pass
                
                # Triger local zeroconf metadata rebuild and notification
                asyncio.create_task(self.discovery.update_local_library_metadata())
                await self.broadcast_event({
                    "event": "media_removed",
                    "data": {"id": media_id}
                })
                return {"success": True}
            return {"success": False}

        else:
            raise NotImplementedError(f"RPC Method {method} not supported")

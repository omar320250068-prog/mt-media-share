import asyncio
import logging
import sys
import threading
import webview
from pathlib import Path
from .config import DOWNLOADS_DIR, DB_PATH, API_PORT, P2P_PORT
from .db.database import Database
from .core.download_engine import DownloadEngine
from .core.library_scanner import LibraryScanner
from .network.transfer_client import TransferClient
from .network.transfer_server import TransferServer
from .network.discovery import DiscoveryManager
from .api.bridge import ApiBridge

# Setup structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("mt.app")

class Application:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.db = Database(DB_PATH)
        
        # Component placeholders
        self.downloader: DownloadEngine | None = None
        self.scanner: LibraryScanner | None = None
        self.server: TransferServer | None = None
        self.client: TransferClient | None = None
        self.discovery: DiscoveryManager | None = None
        self.bridge: ApiBridge | None = None
        self._async_thread: threading.Thread | None = None

    async def _async_start(self):
        """Asynchronously initialize and start all backend subsystems."""
        logger.info("Initializing backend subsystems...")
        
        # 1. Connect database
        await self.db.connect()
        
        # 2. Instantiate systems using cross-communication event handlers
        self.downloader = DownloadEngine(self.db, self._dispatch_event)
        self.client = TransferClient(self.db, self._dispatch_event)
        self.server = TransferServer(self.db)
        
        self.discovery = DiscoveryManager(self.db, self._dispatch_event)
        self.scanner = LibraryScanner(self.db, self._dispatch_event)
        self.bridge = ApiBridge(self.db, self.downloader, self.client, self.discovery)
        
        # 3. Start active managers
        await self.bridge.start(API_PORT)
        await self.server.start(P2P_PORT)
        await self.discovery.start(P2P_PORT)
        await self.scanner.start()
        
        logger.info("Backend components started successfully.")

    async def _async_stop(self):
        """Clean shutdown of backend components."""
        logger.info("Shutting down backend subsystems...")
        if self.scanner:
            await self.scanner.stop()
        if self.discovery:
            await self.discovery.stop()
        if self.server:
            await self.server.stop()
        if self.bridge:
            await self.bridge.stop()
        await self.db.disconnect()
        logger.info("All backend components stopped.")

    async def _dispatch_event(self, event_data: dict):
        """Pass internal system events (P2P progress, downloads) to ApiBridge to forward to UI."""
        if self.bridge:
            await self.bridge.broadcast_event(event_data)

    def _run_async_loop(self):
        """Entry point for the background thread hosting the asyncio loop."""
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._async_start())
        try:
            self.loop.run_forever()
        finally:
            self.loop.run_until_complete(self._async_stop())
            self.loop.close()

    def run(self):
        """Start the application: runs asyncio in thread, blocks main thread with GUI."""
        logger.info("Launching mt Application...")
        
        # Start background asyncio engine thread
        self._async_thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self._async_thread.start()

        # Locate static HTML frontend build
        # In developer mode: frontend/dist/index.html
        # In bundled mode: path relative to script directory
        script_dir = Path(__file__).parent
        dev_frontend_path = script_dir.parent.parent / "frontend" / "dist" / "index.html"
        
        if dev_frontend_path.exists():
            html_url = str(dev_frontend_path.resolve())
            logger.info(f"Loading frontend build index from {html_url}")
        else:
            # Fallback for packaged bundles or missing static builds
            html_url = "data:text/html,<html><body><h1 style='color:white;font-family:sans-serif;'>Frontend build files missing! Please run 'npm run build' inside the frontend folder.</h1></body></html>"
            logger.warning("Vite Svelte static frontend build files missing!")

        # Create pywebview window shell
        window = webview.create_window(
            title="mt — YouTube Downloader & LAN P2P Share",
            url=html_url,
            width=1100,
            height=700,
            min_size=(900, 600),
            background_color="#121214"
        )

        # Start native window process (blocks main thread)
        webview.start()
        
        # When window exits, shut down background thread loop
        logger.info("Window closed. Initiating exit sequences...")
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._async_thread.join(timeout=5)
        logger.info("Exit completed.")

def main():
    app = Application()
    app.run()

if __name__ == "__main__":
    main()

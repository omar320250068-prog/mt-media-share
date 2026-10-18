import asyncio
import socket
import logging
from typing import Callable, Coroutine
from zeroconf import Zeroconf, ServiceInfo, ServiceBrowser, ServiceListener
from ..constants import SERVICE_TYPE, DEFAULT_PORT, PROTOCOL_VERSION
from ..db.database import Database
from ..utils.network import get_local_ip

logger = logging.getLogger("mt.network.discovery")

class DiscoveryManager:
    def __init__(self, db: Database, event_callback: Callable[[dict], Coroutine] = None):
        self.db = db
        self.event_callback = event_callback
        self.zeroconf: Zeroconf | None = None
        self.browser: ServiceBrowser | None = None
        self.service_info: ServiceInfo | None = None
        self.loop = asyncio.get_running_loop()

    async def start(self, local_port: int = DEFAULT_PORT):
        """Register local service and start browsing for peers on LAN."""
        self.zeroconf = Zeroconf()
        
        # 1. Advertise local instance
        hostname = socket.gethostname()
        local_ip = get_local_ip()
        
        # Get count of library files to expose via mDNS properties
        media_items = await self.db.get_all_media()
        lib_count = len(media_items)
        lib_size = sum(item.get("file_size", 0) for item in media_items)

        properties = {
            "version": PROTOCOL_VERSION,
            "device_name": hostname,
            "lib_count": str(lib_count),
            "lib_size": str(lib_size)
        }

        # Pack IP address correctly (Zeroconf expects bytes)
        try:
            ip_bytes = socket.inet_aton(local_ip)
        except Exception:
            ip_bytes = socket.inet_aton("127.0.0.1")

        self.service_info = ServiceInfo(
            SERVICE_TYPE,
            f"mt-{hostname}.{SERVICE_TYPE}",
            addresses=[ip_bytes],
            port=local_port,
            properties=properties,
            server=f"mt-{hostname}.local."
        )

        self.zeroconf.register_service(self.service_info)
        logger.info(f"mDNS advertising service: mt-{hostname} on {local_ip}:{local_port}")

        # 2. Browse for other instances
        listener = PeerDiscoveryListener(self)
        self.browser = ServiceBrowser(self.zeroconf, SERVICE_TYPE, listener)
        logger.info("mDNS browser listening for peer services...")

    async def stop(self):
        """Unregister local service and close browsers."""
        # Mark all peers as offline
        peers = await self.db.get_online_peers()
        for peer in peers:
            await self.db.set_peer_offline(peer["id"])

        if self.zeroconf:
            if self.service_info:
                try:
                    self.zeroconf.unregister_service(self.service_info)
                except Exception:
                    pass
            self.zeroconf.close()
            self.zeroconf = None
            self.browser = None
            logger.info("mDNS discovery manager stopped")

    async def update_local_library_metadata(self):
        """Re-register service with updated library stats."""
        if not self.zeroconf or not self.service_info:
            return
        
        try:
            media_items = await self.db.get_all_media()
            lib_count = len(media_items)
            lib_size = sum(item.get("file_size", 0) for item in media_items)
            
            # Update properties dictionary
            self.service_info.properties.update({
                b"lib_count": str(lib_count).encode("utf-8"),
                b"lib_size": str(lib_size).encode("utf-8")
            })
            
            # Unregister and register again to update parameters
            self.zeroconf.unregister_service(self.service_info)
            self.zeroconf.register_service(self.service_info)
            logger.debug("mDNS local service metadata updated successfully")
        except Exception as e:
            logger.warning(f"Failed to update mDNS properties: {e}")

class PeerDiscoveryListener(ServiceListener):
    def __init__(self, manager: DiscoveryManager):
        self.manager = manager

    def add_service(self, zc: Zeroconf, type_: str, name: str):
        # Ignore our own advertised service
        if self.manager.service_info and name == self.manager.service_info.name:
            return
            
        asyncio.run_coroutine_threadsafe(
            self._handle_add(zc, type_, name),
            self.manager.loop
        )

    def remove_service(self, zc: Zeroconf, type_: str, name: str):
        asyncio.run_coroutine_threadsafe(
            self._handle_remove(zc, type_, name),
            self.manager.loop
        )

    def update_service(self, zc: Zeroconf, type_: str, name: str):
        # Trigger same resolution logic for updates
        self.add_service(zc, type_, name)

    async def _handle_add(self, zc: Zeroconf, type_: str, name: str):
        info = zc.get_service_info(type_, name)
        if not info:
            return

        # Decode addresses
        addresses = []
        for addr in info.addresses:
            try:
                addresses.append(f"{socket.inet_ntoa(addr)}:{info.port}")
            except Exception:
                pass
                
        if not addresses:
            return

        # Decode properties
        props = {}
        for k, v in info.properties.items():
            key = k.decode("utf-8") if isinstance(k, bytes) else k
            val = v.decode("utf-8") if isinstance(v, bytes) else v
            props[key] = val

        peer_id = name.split(".")[0]
        peer_name = props.get("device_name", peer_id)
        
        peer_data = {
            "device_name": peer_name,
            "addresses": addresses,
            "protocol_version": props.get("version", "1"),
            "library_count": int(props.get("lib_count", 0)),
            "library_size": int(props.get("lib_size", 0)),
            "capabilities": "chunk_v1,resume"
        }

        logger.info(f"Peer discovered: {peer_name} at {addresses}")
        await self.manager.db.update_peer(peer_id, peer_data)
        
        if self.manager.event_callback:
            await self.manager.event_callback({
                "event": "peer_discovered",
                "data": {"id": peer_id, **peer_data}
            })

    async def _handle_remove(self, zc: Zeroconf, type_: str, name: str):
        peer_id = name.split(".")[0]
        logger.info(f"Peer offline: {peer_id}")
        await self.manager.db.set_peer_offline(peer_id)
        
        if self.manager.event_callback:
            await self.manager.event_callback({
                "event": "peer_removed",
                "data": {"id": peer_id}
            })

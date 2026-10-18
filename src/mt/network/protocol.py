import asyncio
import struct
import json
import logging
from ..constants import PROTOCOL_MAGIC

logger = logging.getLogger("mt.network.protocol")

HEADER_FORMAT = "!2sBI"  # 2 bytes magic, 1 byte type, 4 bytes uint32 length
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

class ProtocolError(Exception):
    pass

def create_packet(msg_type: int, payload: bytes | str | dict) -> bytes:
    """Compile a protocol packet structure."""
    if isinstance(payload, dict):
        payload_bytes = json.dumps(payload).encode("utf-8")
    elif isinstance(payload, str):
        payload_bytes = payload.encode("utf-8")
    else:
        payload_bytes = payload
        
    length = len(payload_bytes)
    header = struct.pack(HEADER_FORMAT, PROTOCOL_MAGIC, msg_type, length)
    return header + payload_bytes

async def read_packet(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    """Read a packet from the reader stream and validate it."""
    try:
        header_bytes = await reader.readexactly(HEADER_SIZE)
    except asyncio.IncompleteReadError:
        raise ConnectionResetError("Connection closed by peer while reading header")

    magic, msg_type, length = struct.unpack(HEADER_FORMAT, header_bytes)
    
    if magic != PROTOCOL_MAGIC:
        raise ProtocolError(f"Invalid protocol magic byte: {magic.hex()}")

    try:
        payload_bytes = await reader.readexactly(length)
    except asyncio.IncompleteReadError:
        raise ConnectionResetError("Connection closed by peer while reading payload")

    return msg_type, payload_bytes

def parse_json_payload(payload_bytes: bytes) -> dict:
    """Safely decode and parse JSON payload."""
    try:
        return json.loads(payload_bytes.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ProtocolError(f"Failed to decode JSON payload: {e}")

import socket

# Wire Protocol
PROTOCOL_VERSION = "1"
PROTOCOL_MAGIC = b"\x4D\x54"  # "MT" in hex
DEFAULT_PORT = 8400
SERVICE_TYPE = "_mt._tcp.local."

# Chunk size for transfer: 1 MB
CHUNK_SIZE = 1024 * 1024  # 1,048,576 bytes
HASH_BUF_SIZE = 65536     # 64 KB for hashing stream buffer

# Connection timeouts
PEER_TIMEOUT = 30.0       # Seconds before peer is marked dead
KEEPALIVE_INTERVAL = 10.0 # Ping interval

# DB
DB_FILE_NAME = "mt_library.db"

# Message type enums
MSG_CATALOG_REQ = 0x01
MSG_CATALOG_RES = 0x02
MSG_TRANSFER_REQ = 0x10
MSG_TRANSFER_META = 0x11
MSG_CHUNK_HEADER = 0x12
MSG_CHUNK_DATA = 0x13
MSG_CHUNK_ACK = 0x14
MSG_TRANSFER_COMPLETE = 0x15
MSG_TRANSFER_CANCEL = 0x20
MSG_PING = 0x30
MSG_PONG = 0x31
MSG_ERROR = 0xFF

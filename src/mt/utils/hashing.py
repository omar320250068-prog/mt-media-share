import hashlib
from pathlib import Path
from ..constants import HASH_BUF_SIZE

def compute_file_hash(filepath: Path) -> str:
    """Stream-hash a file to compute its SHA-256 digest without loading the whole file into memory."""
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(HASH_BUF_SIZE):
            sha256.update(chunk)
    return sha256.hexdigest()

def compute_chunk_hash(chunk_data: bytes) -> str:
    """Compute the SHA-256 digest of a block of bytes."""
    return hashlib.sha256(chunk_data).hexdigest()

def get_file_fast_identity(filepath: Path) -> tuple[float, int]:
    """Return mtime and size of file for fast changes checking."""
    stats = filepath.stat()
    return stats.st_mtime, stats.st_size

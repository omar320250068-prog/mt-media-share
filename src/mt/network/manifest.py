import json
import logging
from pathlib import Path
from typing import TypedDict, Dict

logger = logging.getLogger("mt.network.manifest")

class ManifestData(TypedDict):
    version: int
    file_id: str
    file_name: str
    file_size: int
    file_sha256: str
    chunk_size: int
    total_chunks: int
    completed_chunks: Dict[str, str]  # stringified chunk_index -> chunk_hash
    source_peer_id: str
    started_at: str
    last_updated: str
    partial_file_path: str

def create_manifest_file(
    manifest_path: Path,
    file_id: str,
    file_name: str,
    file_size: int,
    file_sha256: str,
    chunk_size: int,
    source_peer_id: str,
    started_at: str,
    partial_file_path: str
) -> ManifestData:
    """Instantiate a new transfer resume manifest."""
    total_chunks = (file_size + chunk_size - 1) // chunk_size
    manifest: ManifestData = {
        "version": 1,
        "file_id": file_id,
        "file_name": file_name,
        "file_size": file_size,
        "file_sha256": file_sha256,
        "chunk_size": chunk_size,
        "total_chunks": total_chunks,
        "completed_chunks": {},
        "source_peer_id": source_peer_id,
        "started_at": started_at,
        "last_updated": started_at,
        "partial_file_path": partial_file_path
    }
    write_manifest(manifest_path, manifest)
    return manifest

def write_manifest(path: Path, data: ManifestData):
    """Save manifest state to disk."""
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to write manifest at {path.name}: {e}")

def read_manifest(path: Path) -> ManifestData | None:
    """Load manifest structure from disk."""
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to parse manifest at {path.name}: {e}")
        return None

def get_next_missing_chunk(manifest: ManifestData) -> int:
    """Determine the index of the first incomplete chunk."""
    total = manifest["total_chunks"]
    completed = manifest["completed_chunks"]
    for i in range(total):
        if str(i) not in completed:
            return i
    return total

import os
import sys
from pathlib import Path

# Base Paths
USER_HOME = Path.home()

# Determine standard Downloads/Videos directory
def get_default_downloads_dir() -> Path:
    videos_dir = USER_HOME / "Videos" / "mt"
    downloads_dir = USER_HOME / "Downloads" / "mt"
    
    # Prefer Videos, then Downloads, fallback to standard user home directory
    if (USER_HOME / "Videos").exists():
        return videos_dir
    elif (USER_HOME / "Downloads").exists():
        return downloads_dir
    else:
        return USER_HOME / "mt_downloads"

# Application storage for internal state (db, cached thumbnails)
def get_app_data_dir() -> Path:
    if sys.platform == "win32":
        base = Path(os.environ.get("APPDATA", USER_HOME / "AppData" / "Roaming"))
    elif sys.platform == "darwin":
        base = USER_HOME / "Library" / "Application Support"
    else:
        base = Path(os.environ.get("XDG_DATA_HOME", USER_HOME / ".local" / "share"))
        
    app_dir = base / "mt"
    app_dir.mkdir(parents=True, exist_ok=True)
    return app_dir

# Resolve configuration variables
DOWNLOADS_DIR = get_default_downloads_dir()
APP_DATA_DIR = get_app_data_dir()
DB_PATH = APP_DATA_DIR / "mt_library.db"
THUMBNAILS_DIR = APP_DATA_DIR / "thumbnails"

# Ensure direct directories exist
DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
THUMBNAILS_DIR.mkdir(parents=True, exist_ok=True)

# Port configuration
API_PORT = 8399  # WebSocket server port
P2P_PORT = 8400  # TCP Transfer server port

# Ensure ffmpeg discovery helper
def get_ffmpeg_path() -> str | None:
    # Check if bundled or in PATH
    # Standard check: pyinstaller sys._MEIPASS
    if getattr(sys, 'frozen', False):
        bundle_dir = Path(sys._MEIPASS)
        # Check standard bundling locations
        ffmpeg_bin = bundle_dir / "ffmpeg.exe" if sys.platform == "win32" else bundle_dir / "ffmpeg"
        if ffmpeg_bin.exists():
            return str(ffmpeg_bin)
    
    # Rely on system PATH
    return None

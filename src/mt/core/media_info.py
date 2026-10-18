import subprocess
import json
import logging
from pathlib import Path
from ..config import get_ffmpeg_path

logger = logging.getLogger("mt.probe")

def probe_media_file(filepath: Path) -> dict:
    """Run ffprobe on file to get resolution, codec, duration details."""
    ffprobe_cmd = "ffprobe"
    
    # Bundle fallback check
    ffmpeg_path = get_ffmpeg_path()
    if ffmpeg_path:
        # If ffmpeg is bundled, try to find ffprobe in same folder
        ffprobe_bin = Path(ffmpeg_path).parent / ("ffprobe.exe" if Path(ffmpeg_path).suffix == ".exe" else "ffprobe")
        if ffprobe_bin.exists():
            ffprobe_cmd = str(ffprobe_bin)

    cmd = [
        ffprobe_cmd,
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        str(filepath.resolve())
    ]
    
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        data = json.loads(result.stdout)
        
        info = {
            "duration": 0.0,
            "width": None,
            "height": None,
            "fps": None,
            "video_codec": None,
            "audio_codec": None,
            "container": filepath.suffix.lstrip("."),
            "bitrate": 0
        }
        
        # Parse format
        fmt = data.get("format", {})
        info["duration"] = float(fmt.get("duration", 0.0))
        info["bitrate"] = int(fmt.get("bit_rate", 0) or 0)
        
        # Parse streams
        for stream in data.get("streams", []):
            codec_type = stream.get("codec_type")
            if codec_type == "video" and not info["video_codec"]:
                info["video_codec"] = stream.get("codec_name")
                info["width"] = int(stream.get("width") or 0) or None
                info["height"] = int(stream.get("height") or 0) or None
                
                # Compute FPS (average_frame_rate is e.g. "30/1" or "24000/1001")
                fps_str = stream.get("avg_frame_rate", "")
                if "/" in fps_str:
                    try:
                        num, den = map(float, fps_str.split("/"))
                        if den > 0:
                            info["fps"] = round(num / den, 2)
                    except ValueError:
                        pass
            elif codec_type == "audio" and not info["audio_codec"]:
                info["audio_codec"] = stream.get("codec_name")
                
        return info
    except Exception as e:
        logger.error(f"Failed to probe file {filepath.name}: {e}")
        # Return fallback details based on file properties
        return {
            "duration": 0.0,
            "width": None,
            "height": None,
            "fps": None,
            "video_codec": None,
            "audio_codec": None,
            "container": filepath.suffix.lstrip("."),
            "bitrate": 0
        }

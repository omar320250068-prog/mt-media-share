import socket

def get_local_ip() -> str:
    """Return the primary local network IPv4 address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Does not send actual packet, just resolves interface IP towards destination
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        # Fallback to resolving hostname
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            ip = "127.0.0.1"
    finally:
        s.close()
    return ip

def is_port_available(port: int) -> bool:
    """Check if a local TCP port is free."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("0.0.0.0", port))
        available = True
    except Exception:
        available = False
    finally:
        s.close()
    return available

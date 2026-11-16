"""
Configuration for the Jellyfin caching proxy.
Values can be set via environment variables or a .env file.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Config:
    # URL of the remote (main) Jellyfin server, e.g. https://jellyfin.example.com
    remote_url: str = ""

    # API key for the remote Jellyfin server (used by the downloader)
    remote_api_key: str = ""

    # User ID associated with the API key above (used for metadata lookups).
    # If left empty, the proxy will auto-discover it on startup by calling /Users/Me.
    remote_user_id: str = ""

    # Where to store cached media files locally
    cache_dir: Path = Path("/mnt/jellyfin-cache")

    # Maximum cache size in GB (0 = unlimited)
    cache_max_gb: float = 0

    # How long to keep a fully-watched file before evicting (hours, 0 = keep forever)
    evict_after_watched_hours: float = 24

    # Port this proxy listens on
    listen_port: int = 8096

    # How many concurrent downloads to allow
    max_concurrent_downloads: int = 1

    # Whether HLS segment requests should be served by local FFmpeg from cache.
    # Disabled by default because many TV/browser clients are more stable when
    # HLS transcoding stays on the remote Jellyfin server.
    enable_local_hls_transcode: bool = False

    # Session TTL for local HLS transcode processes.
    local_hls_session_ttl_seconds: int = 300

    # Enable Nvidia acceleration in local FFmpeg HLS sessions.
    local_hls_use_nvidia: bool = False

    # Usernames or user IDs to prefetch/evict-check for (empty list = all users)
    prefetch_users: list = field(default_factory=list)
    prefetch_favourite_episodes: int = 3

    @property
    def remote_auth_headers(self) -> dict[str, str]:
        return {"Authorization": f'MediaBrowser Token="{self.remote_api_key}"'}

    @classmethod
    def load(cls) -> "Config":
        _load_dotenv()
        raw_users = os.getenv("JELLYFIN_PREFETCH_USERS", "")
        prefetch_users = [u.strip() for u in raw_users.split(",") if u.strip()]
        return cls(
            remote_url=_require("JELLYFIN_REMOTE_URL").rstrip("/"),
            remote_api_key=_require("JELLYFIN_REMOTE_API_KEY"),
            remote_user_id=_require("JELLYFIN_REMOTE_USER_ID"),
            cache_dir=Path(os.getenv("JELLYFIN_CACHE_DIR", "/mnt/jellyfin-cache")),
            cache_max_gb=float(os.getenv("JELLYFIN_CACHE_MAX_GB", "0")),
            evict_after_watched_hours=float(
                os.getenv("JELLYFIN_EVICT_AFTER_WATCHED_HOURS", "24")
            ),
            listen_port=int(os.getenv("JELLYFIN_PROXY_PORT", "8096")),
            max_concurrent_downloads=int(
                os.getenv("JELLYFIN_MAX_CONCURRENT_DOWNLOADS", "1")
            ),
            enable_local_hls_transcode=_env_bool(
                os.getenv("JELLYFIN_ENABLE_LOCAL_HLS_TRANSCODE", "0")
            ),
            local_hls_session_ttl_seconds=int(
                os.getenv("JELLYFIN_LOCAL_HLS_SESSION_TTL_SECONDS", "300")
            ),
            local_hls_use_nvidia=_env_bool(
                os.getenv("JELLYFIN_LOCAL_HLS_USE_NVIDIA", "0")
            ),
            prefetch_users=prefetch_users,
            prefetch_favourite_episodes=int(
                os.getenv("JELLYFIN_PREFETCH_FAVOURITE_EPISODES", "3")
            ),
        )


def _env_bool(val: str) -> bool:
    return val.strip().lower() in {"1", "true", "yes", "on"}


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Required environment variable {key!r} is not set")
    return val


def _load_dotenv():
    """Minimal .env loader — no external dependency needed."""
    env_file = Path(".env")
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val
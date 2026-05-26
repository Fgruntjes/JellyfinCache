"""
Main entrypoint
===============
Starts the proxy server and prefetch daemon together.

Usage:
    python main.py

Or run them separately:
    uvicorn proxy:app --host 0.0.0.0 --port 8096
    python prefetch.py
"""

import asyncio
import logging
import os

import uvicorn

from prefetch import PrefetchDaemon
from proxy import app, cfg, download_manager

log = logging.getLogger("main")


async def run_all():
    cfg.cache_dir.mkdir(parents=True, exist_ok=True)

    prefetch_daemon = PrefetchDaemon(cfg, download_manager)
    interval = int(os.getenv("JELLYFIN_PREFETCH_INTERVAL_SECONDS", "3600"))

    server_config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=cfg.listen_port,
        log_level="info",
    )
    server = uvicorn.Server(server_config)

    log.info("Starting Jellyfin caching proxy on port %d", cfg.listen_port)
    log.info("Remote Jellyfin: %s", cfg.remote_url)
    log.info("Cache dir: %s", cfg.cache_dir)

    await download_manager.startup_resume()

    await asyncio.gather(
        server.serve(),
        prefetch_daemon.run(interval_seconds=interval),
    )


if __name__ == "__main__":
    asyncio.run(run_all())

"""
Download Manager
================
Handles fetching complete media files from the remote Jellyfin server
into the local cache directory.

File layout for item <id>:
  cache_dir/<id>/.downloading    - lock file, present while download is in progress
  cache_dir/<id>/<filename>      - the actual media file (written atomically on completion)
  cache_dir/<id>/.integrity.json - checksum metadata for integrity validation
  cache_dir/<id>/.ready          - marker file containing <filename>, written last

A file is considered "available" only when .ready exists.
"""

import asyncio
import hashlib
import json
import logging
import time
from pathlib import Path

import httpx

from config import Config

log = logging.getLogger("downloader")

_SAMPLE_INTERVAL = 5.0      # seconds between speed-history samples
_HISTORY_MAX_HOURS = 24     # oldest samples to retain
_INTEGRITY_FILENAME = ".integrity.json"
_INTEGRITY_ALGO = "sha256"


class _RangeNotSatisfiable(Exception):
    """Raised when the server returns 416 and our .tmp is larger than the file."""


class DownloadManager:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._in_progress: set[str] = set()    # queued + actively downloading
        self._active: set[str] = set()         # inside semaphore, currently transferring
        self._progress: dict[str, dict] = {}   # item_id -> {downloaded, total, filename}
        self._history: dict[str, list[tuple[float, int]]] = {}  # item_id -> [(unix_ts, bytes)]
        self._semaphore = asyncio.Semaphore(cfg.max_concurrent_downloads)
        self._user_id: str = cfg.remote_user_id

    @staticmethod
    def _normalize_item_id(item_id: str) -> str:
        return item_id.replace("-", "")

    def _speed_history_path(self) -> Path:
        return self.cfg.cache_dir / ".speed_history.json"

    def _integrity_path(self, item_dir: Path) -> Path:
        return item_dir / _INTEGRITY_FILENAME

    def _hash_file(self, path: Path) -> str:
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()

    def _read_integrity(self, item_dir: Path) -> dict | None:
        integrity_path = self._integrity_path(item_dir)
        if not integrity_path.exists():
            return None
        try:
            data = json.loads(integrity_path.read_text())
            if isinstance(data, dict):
                return data
        except Exception:
            pass
        return None

    def _write_integrity(self, item_dir: Path, filename: str, size: int, checksum: str) -> None:
        integrity_path = self._integrity_path(item_dir)
        payload = {
            "algorithm": _INTEGRITY_ALGO,
            "filename": filename,
            "size": size,
            "checksum": checksum,
            "created_at": int(time.time()),
        }
        tmp = integrity_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload))
        tmp.rename(integrity_path)

    def _invalidate_ready_entry(self, item_dir: Path, reason: str) -> None:
        log.warning("Invalid cache entry %s: %s", item_dir.name, reason)
        (item_dir / ".ready").unlink(missing_ok=True)
        self._integrity_path(item_dir).unlink(missing_ok=True)

    def _validate_or_create_integrity(self, item_dir: Path) -> bool:
        marker = item_dir / ".ready"
        if not marker.exists():
            return False

        filename = marker.read_text().strip()
        if not filename:
            self._invalidate_ready_entry(item_dir, "empty .ready marker")
            return False

        media_file = item_dir / filename
        if not media_file.exists() or media_file.stat().st_size <= 0:
            self._invalidate_ready_entry(item_dir, "missing or empty media file")
            return False

        file_size = media_file.stat().st_size
        integrity = self._read_integrity(item_dir)

        # Migration: create integrity metadata for existing healthy entries.
        if integrity is None:
            checksum = self._hash_file(media_file)
            self._write_integrity(item_dir, filename, file_size, checksum)
            log.info("Created integrity metadata for cached item: %s", item_dir.name)
            return True

        if integrity.get("algorithm") != _INTEGRITY_ALGO:
            self._invalidate_ready_entry(item_dir, "unsupported checksum algorithm")
            return False

        if integrity.get("filename") != filename:
            self._invalidate_ready_entry(item_dir, "filename mismatch")
            return False

        expected_size = int(integrity.get("size", -1))
        if expected_size != file_size:
            self._invalidate_ready_entry(
                item_dir,
                f"size mismatch (actual={file_size}, expected={expected_size})",
            )
            return False

        expected_checksum = str(integrity.get("checksum", "")).strip().lower()
        actual_checksum = self._hash_file(media_file)
        if not expected_checksum or actual_checksum != expected_checksum:
            self._invalidate_ready_entry(item_dir, "checksum mismatch")
            return False

        return True

    def _load_speed_history(self) -> None:
        path = self._speed_history_path()
        if not path.exists():
            return
        try:
            raw: dict[str, list] = json.loads(path.read_text())
            cutoff = time.time() - _HISTORY_MAX_HOURS * 3600
            for item_id, samples in raw.items():
                trimmed = [(t, b) for t, b in samples if t >= cutoff]
                if trimmed:
                    self._history[item_id] = trimmed
        except Exception:
            pass

    def _save_speed_history(self) -> None:
        path = self._speed_history_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            cutoff = time.time() - _HISTORY_MAX_HOURS * 3600
            to_save = {
                iid: [(t, b) for t, b in hist if t >= cutoff]
                for iid, hist in self._history.items()
            }
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(to_save))
            tmp.rename(path)
        except Exception:
            pass

    def queue_snapshot(self) -> dict:
        """Return a point-in-time snapshot of queue state for the dashboard."""
        return {
            "queued": list(self._in_progress - self._active),
            "active": list(self._active),
            "progress": {k: dict(v) for k, v in self._progress.items()},
            "history": {k: list(v) for k, v in self._history.items()},
        }

    def _resolve_user_id(self) -> str:
        if not self._user_id:
            raise RuntimeError(
                "JELLYFIN_REMOTE_USER_ID is not set. "
                "Find your user ID in the Jellyfin dashboard (Dashboard -> Users -> "
                "click your user -> copy the ID from the URL)."
            )
        return self._user_id

    async def startup_resume(self) -> None:
        """Load persisted state, validate ready cache entries, and resume stale downloads."""
        self.cfg.cache_dir.mkdir(parents=True, exist_ok=True)
        self._load_speed_history()

        if not self.cfg.cache_dir.exists():
            return

        for item_dir in self.cfg.cache_dir.iterdir():
            if not item_dir.is_dir():
                continue

            ready = item_dir / ".ready"
            if ready.exists():
                self._validate_or_create_integrity(item_dir)

            lock = item_dir / ".downloading"
            if not lock.exists() or ready.exists():
                continue

            # Stale lock from a previous run - check for a partial .tmp.
            tmp_files = list(item_dir.glob("*.tmp"))
            lock.unlink(missing_ok=True)

            if tmp_files:
                log.info(
                    "Resuming interrupted download: %s (%s bytes done)",
                    item_dir.name,
                    tmp_files[0].stat().st_size,
                )
            else:
                log.info("Re-enqueuing incomplete download: %s", item_dir.name)

            asyncio.create_task(self.enqueue(self._normalize_item_id(item_dir.name)))

    async def enqueue(self, item_id: str):
        """Schedule a download for item_id if not already cached or in progress."""
        item_id = self._normalize_item_id(item_id)
        if item_id in self._in_progress:
            return

        item_dir = self.cfg.cache_dir / item_id
        if (item_dir / ".ready").exists() and self._validate_or_create_integrity(item_dir):
            return

        self._in_progress.add(item_id)
        try:
            await self._download(item_id)
        except Exception as e:
            log.error("Download failed for %s: %s", item_id, e)
        finally:
            self._in_progress.discard(item_id)
            self._active.discard(item_id)
            self._progress.pop(item_id, None)
            # Keep history in memory so graph stays populated; it's also on disk.
            self._save_speed_history()

    async def _download(self, item_id: str):
        item_id = self._normalize_item_id(item_id)
        async with self._semaphore:
            self._active.add(item_id)
            item_dir = self.cfg.cache_dir / item_id
            item_dir.mkdir(parents=True, exist_ok=True)

            lock = item_dir / ".downloading"
            if lock.exists():
                log.info("Already downloading %s (lock exists)", item_id)
                return

            lock.touch()
            log.info("Starting download: %s", item_id)

            try:
                filename, url = await self._resolve_stream(item_id)
                if not filename or not url:
                    log.warning("Could not resolve stream for %s", item_id)
                    return

                dest = item_dir / filename
                tmp_path = dest.with_suffix(".tmp")

                try:
                    checksum, size = await self._fetch_to_file(url, dest, item_id=item_id)
                except _RangeNotSatisfiable:
                    # .tmp was bigger than the remote file - delete and restart.
                    tmp_path.unlink(missing_ok=True)
                    checksum, size = await self._fetch_to_file(url, dest, item_id=item_id)

                self._write_integrity(item_dir, filename=filename, size=size, checksum=checksum)

                # Write ready marker atomically after integrity metadata is recorded.
                marker = item_dir / ".ready"
                marker.write_text(filename)
                log.info("Download complete: %s -> %s", item_id, dest)

                await self._enforce_cache_limit()

            finally:
                lock.unlink(missing_ok=True)

    async def _resolve_stream(self, item_id: str) -> tuple[str, str]:
        """
        Ask the remote Jellyfin for the best direct-play stream URL and filename.
        Returns (filename, url).
        """
        headers = self.cfg.remote_auth_headers
        base = self.cfg.remote_url
        user_id = self._resolve_user_id()

        async with httpx.AsyncClient(timeout=15) as client:
            meta_resp = await client.get(
                f"{base}/Users/{user_id}/Items/{item_id}",
                headers=headers,
            )
            if meta_resp.status_code != 200:
                log.warning(
                    "Could not fetch metadata for %s: HTTP %s - %s",
                    item_id,
                    meta_resp.status_code,
                    meta_resp.text[:200],
                )
                return "", ""

            meta = meta_resp.json()
            filename = meta.get("Path", "")
            if filename:
                filename = Path(filename).name
            else:
                filename = f"{item_id}.mkv"

            url = f"{base}/Items/{item_id}/Download"

        return filename, url

    async def _fetch_to_file(self, url: str, dest: Path, item_id: str = "") -> tuple[str, int]:
        """Download url into dest and return (sha256_checksum, byte_size)."""
        tmp_path = dest.with_suffix(".tmp")
        resume_from = tmp_path.stat().st_size if tmp_path.exists() else 0

        req_headers = dict(self.cfg.remote_auth_headers)
        hasher = hashlib.sha256()
        if resume_from:
            req_headers["Range"] = f"bytes={resume_from}-"
            log.info("Resuming %s from byte %d", dest.name, resume_from)
            # Include existing partial bytes in the final checksum state.
            with open(tmp_path, "rb") as existing:
                while True:
                    chunk = existing.read(1024 * 1024)
                    if not chunk:
                        break
                    hasher.update(chunk)

        try:
            async with httpx.AsyncClient(timeout=None, follow_redirects=True) as client:
                async with client.stream("GET", url, headers=req_headers) as resp:
                    if resp.status_code == 416:
                        raise _RangeNotSatisfiable()
                    resp.raise_for_status()

                    is_partial = resp.status_code == 206
                    total_header = int(resp.headers.get("content-length", 0))

                    if is_partial:
                        # Content-Range: bytes start-end/total
                        cr = resp.headers.get("content-range", "")
                        try:
                            total = int(cr.split("/")[-1])
                        except (ValueError, IndexError):
                            total = total_header + resume_from
                        downloaded = resume_from
                        file_mode = "ab"
                    else:
                        # Server ignored Range or no resume - full download.
                        total = total_header
                        downloaded = 0
                        resume_from = 0
                        file_mode = "wb"
                        hasher = hashlib.sha256()

                    log.info(
                        "Downloading %s -> %s (%.1f MB total%s)",
                        url.split("?")[0].split("/")[-2],
                        dest.name,
                        total / 1024 / 1024 if total else 0,
                        f", resuming from {resume_from / 1024 / 1024:.1f} MB" if resume_from else "",
                    )

                    if item_id:
                        t0 = time.time()
                        # Seed history with current progress so graph is continuous.
                        if item_id not in self._history:
                            self._history[item_id] = [(t0, downloaded)]
                        else:
                            self._history[item_id].append((t0, downloaded))
                        self._progress[item_id] = {
                            "downloaded": downloaded,
                            "total": total,
                            "filename": dest.name,
                        }
                        last_sample_t = t0
                        last_save_t = t0

                    with open(tmp_path, file_mode) as f:
                        async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                            f.write(chunk)
                            downloaded += len(chunk)
                            hasher.update(chunk)

                            if item_id:
                                self._progress[item_id]["downloaded"] = downloaded
                                t = time.time()
                                if t - last_sample_t >= _SAMPLE_INTERVAL:
                                    self._history[item_id].append((t, downloaded))
                                    last_sample_t = t
                                    cutoff = t - _HISTORY_MAX_HOURS * 3600
                                    hist = self._history[item_id]
                                    while len(hist) > 2 and hist[0][0] < cutoff:
                                        hist.pop(0)
                                # Persist speed history every 60 s.
                                if t - last_save_t >= 60:
                                    self._save_speed_history()
                                    last_save_t = t

                            if total:
                                pct = downloaded / total * 100
                                if downloaded % (50 * 1024 * 1024) < 1024 * 1024:
                                    log.info(
                                        "  %.1f%%  (%.0f / %.0f MB)",
                                        pct,
                                        downloaded / 1024**2,
                                        total / 1024**2,
                                    )

            # Atomic rename.
            tmp_path.rename(dest)

            final_size = dest.stat().st_size
            if total and final_size != total:
                raise RuntimeError(
                    f"Downloaded size mismatch for {dest.name}: got {final_size}, expected {total}"
                )

            stream_checksum = hasher.hexdigest()
            # Verify bytes on disk match what was streamed.
            disk_checksum = self._hash_file(dest)
            if disk_checksum != stream_checksum:
                raise RuntimeError(
                    f"Checksum verification failed for {dest.name}: {disk_checksum} != {stream_checksum}"
                )

            return disk_checksum, final_size

        except _RangeNotSatisfiable:
            raise
        except Exception:
            # Leave tmp_path in place so next startup can resume from it.
            raise

    async def _enforce_cache_limit(self):
        """If cache_max_gb is set, evict oldest ready items until under the limit."""
        if self.cfg.cache_max_gb <= 0:
            return

        limit_bytes = self.cfg.cache_max_gb * 1024**3

        items = []
        for item_dir in self.cfg.cache_dir.iterdir():
            if not item_dir.is_dir():
                continue
            marker = item_dir / ".ready"
            if not marker.exists():
                continue
            fname = marker.read_text().strip()
            fpath = item_dir / fname
            if fpath.exists():
                items.append((marker.stat().st_mtime, item_dir, fpath))

        items.sort()  # oldest first
        total = sum(fp.stat().st_size for _, _, fp in items)

        for _, item_dir, fpath in items:
            if total <= limit_bytes:
                break
            size = fpath.stat().st_size
            log.info("Cache limit: evicting %s (%.1f MB)", item_dir.name, size / 1024**2)
            fpath.unlink(missing_ok=True)
            (item_dir / ".ready").unlink(missing_ok=True)
            self._integrity_path(item_dir).unlink(missing_ok=True)
            total -= size

    async def evict_item(self, item_id: str):
        """Remove a cached item from disk."""
        import shutil

        item_dir = self.cfg.cache_dir / item_id
        if item_dir.exists():
            shutil.rmtree(item_dir)
            log.info("Evicted %s from cache", item_id)

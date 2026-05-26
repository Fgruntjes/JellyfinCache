"""
Web Panel
=========
Dashboard at /proxy/panel showing active sessions, downloads, cached
library, and disk usage.

Wired up in proxy.py (before the catch-all route) via:
    panel.setup(cfg, download_manager)
    app.include_router(panel.router)
"""

import asyncio
import json
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader, select_autoescape

from config import Config
from downloader import DownloadManager

router = APIRouter()

_cfg: Config | None = None
_dm: DownloadManager | None = None

# item_id → (fetched_at_unix, metadata_dict)
_meta_cache: dict[str, tuple[float, dict]] = {}
# How long before we try to refresh from the server (background)
_META_FRESH_TTL = 300.0   # 5 minutes
# How long disk-cached metadata is considered usable without a network refresh
_META_DISK_TTL = 7 * 86400.0  # 7 days

# full dashboard data cache
_data_cache: tuple[float, dict] | None = None
_DATA_TTL = 8.0

_jinja_env: Environment | None = None


def _meta_cache_path() -> Path:
    return _cfg.cache_dir / ".meta_cache.json"


def _load_meta_cache() -> None:
    """Load persisted metadata from disk into _meta_cache on startup."""
    path = _meta_cache_path()
    if not path.exists():
        return
    try:
        raw = json.loads(path.read_text())
        now = time.time()
        for item_id, (fetched_at, meta) in raw.items():
            # Discard entries older than _META_DISK_TTL
            if now - fetched_at < _META_DISK_TTL:
                _meta_cache[item_id] = (fetched_at, meta)
    except Exception:
        pass


def _save_meta_cache() -> None:
    """Persist _meta_cache to disk atomically."""
    path = _meta_cache_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(_meta_cache))
        tmp.rename(path)
    except Exception:
        pass


def setup(cfg: Config, dm: DownloadManager) -> None:
    global _cfg, _dm
    _cfg = cfg
    _dm = dm
    _load_meta_cache()


# ── Jinja2 env ────────────────────────────────────────────────────────────────

def _get_env() -> Environment:
    global _jinja_env
    if _jinja_env is None:
        tmpl_dir = Path(__file__).parent / "templates"
        _jinja_env = Environment(
            loader=FileSystemLoader(str(tmpl_dir)),
            autoescape=select_autoescape(["html"]),
        )
        _jinja_env.filters["filesize"] = _fmt_size
    return _jinja_env


def _fmt_size(n: int) -> str:
    if n < 1024 ** 3:
        return f"{n / 1024 ** 2:.0f} MB"
    return f"{n / 1024 ** 3:.2f} GB"


# ── Formatting helpers ─────────────────────────────────────────────────────────

def _fmt_runtime(ticks: int | None) -> str:
    if not ticks:
        return ""
    s = ticks // 10_000_000
    h, r = divmod(s, 3600)
    m = r // 60
    return f"{h}h {m:02d}m" if h else f"{m}m"


def _item_label(meta: dict) -> str:
    name = meta.get("Name", "Unknown")
    if meta.get("Type") == "Episode":
        series = meta.get("SeriesName", "")
        season = meta.get("ParentIndexNumber")
        ep = meta.get("IndexNumber")
        parts = [series]
        if season is not None and ep is not None:
            parts.append(f"S{season:02d}E{ep:02d}")
        parts.append(name)
        return " · ".join(p for p in parts if p)
    return name


def _item_size_bytes(meta: dict) -> int:
    """Best-effort file size from metadata (MediaSources[0].Size)."""
    sources = meta.get("MediaSources", [])
    return sources[0].get("Size", 0) if sources else 0


def _poster_url(item_id: str, tag: str | None = None) -> str:
    url = f"/Items/{item_id}/Images/Primary?fillWidth=200&quality=80"
    if tag:
        url += f"&tag={tag}"
    return url


# ── Remote data fetching ───────────────────────────────────────────────────────

async def _fetch_meta(item_id: str) -> dict:
    """Fetch metadata from remote, update in-memory and disk cache. Returns
    whatever we have (fresh, stale, or empty) immediately after the network call."""
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                f"{_cfg.remote_url}/Users/{_cfg.remote_user_id}/Items/{item_id}",
                headers={"X-Emby-Token": _cfg.remote_api_key},
            )
            if r.status_code == 200:
                meta = r.json()
                _meta_cache[item_id] = (time.time(), meta)
                return meta
    except Exception:
        pass
    # Return stale data if we have it
    entry = _meta_cache.get(item_id)
    return entry[1] if entry else {}


async def _refresh_meta_batch(ids: list[str]) -> None:
    """Background task: refresh metadata for a list of IDs, then save to disk."""
    if not ids:
        return
    await asyncio.gather(*[_fetch_meta(iid) for iid in ids])
    _save_meta_cache()


def _meta_for(item_id: str) -> dict:
    """Return whatever metadata we have in memory right now (possibly stale or empty)."""
    entry = _meta_cache.get(item_id)
    return entry[1] if entry else {}


def _is_meta_fresh(item_id: str) -> bool:
    entry = _meta_cache.get(item_id)
    if not entry:
        return False
    return time.time() - entry[0] < _META_FRESH_TTL


async def _fetch_sessions() -> list[dict]:
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                f"{_cfg.remote_url}/Sessions",
                headers={"X-Emby-Token": _cfg.remote_api_key},
                params={"ActiveWithinSeconds": 60},
            )
            if r.status_code == 200:
                return r.json()
    except Exception:
        pass
    return []


async def _fetch_watchers() -> dict[str, list[str]]:
    """Return {item_id: [user_name, ...]} for every item in any user's
    Continue Watching or Next Up list. Used to explain why an item is cached."""
    mapping: dict[str, list[str]] = {}
    headers = {"X-Emby-Token": _cfg.remote_api_key}
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            users_r = await c.get(f"{_cfg.remote_url}/Users", headers=headers)
            if users_r.status_code != 200:
                return mapping
            users = users_r.json()
            sem = asyncio.Semaphore(8)

            async def _fetch_user(user: dict) -> tuple[str, set[str]]:
                uid = user["Id"]
                name = user.get("Name", uid)
                async with sem:
                    resume_r, nextup_r = await asyncio.gather(
                        c.get(
                            f"{_cfg.remote_url}/Users/{uid}/Items/Resume",
                            headers=headers,
                            params={"Limit": 50, "MediaTypes": "Video"},
                        ),
                        c.get(
                            f"{_cfg.remote_url}/Shows/NextUp",
                            headers=headers,
                            params={"UserId": uid, "Limit": 50},
                        ),
                    )

                item_ids: set[str] = set()
                if resume_r.status_code == 200:
                    item_ids.update(
                        item["Id"] for item in resume_r.json().get("Items", []) if "Id" in item
                    )
                if nextup_r.status_code == 200:
                    item_ids.update(
                        item["Id"] for item in nextup_r.json().get("Items", []) if "Id" in item
                    )
                return name, item_ids

            results = await asyncio.gather(
                *[_fetch_user(user) for user in users],
                return_exceptions=True,
            )

            for result in results:
                if isinstance(result, Exception):
                    continue
                name, item_ids = result
                for item_id in item_ids:
                    mapping.setdefault(item_id, [])
                    if name not in mapping[item_id]:
                        mapping[item_id].append(name)
    except Exception:
        pass
    return mapping


# ── Dashboard data assembly ────────────────────────────────────────────────────

async def _build_data() -> dict:
    cfg = _cfg
    dm = _dm
    now = datetime.now(timezone.utc)

    # Disk usage
    try:
        du = shutil.disk_usage(cfg.cache_dir if cfg.cache_dir.exists() else "/")
        disk_total, disk_used, disk_free = du.total, du.used, du.free
    except OSError:
        disk_total = disk_used = disk_free = 0

    # Scan cache directory for ready and in-progress items
    cached_raw: list[dict] = []
    cache_bytes = 0
    if cfg.cache_dir.exists():
        for d in cfg.cache_dir.iterdir():
            if not d.is_dir():
                continue
            marker = d / ".ready"
            if marker.exists():
                fname = marker.read_text().strip()
                fpath = d / fname
                sz = fpath.stat().st_size if fpath.exists() else 0
                cache_bytes += sz
                cached_raw.append({"item_id": d.name, "filename": fname, "size_bytes": sz})

    # Download manager state
    snap = dm.queue_snapshot()
    queued_ids: list[str] = snap["queued"]
    active_ids: list[str] = snap["active"]
    progress_map: dict[str, dict] = snap["progress"]

    # Partition item IDs: stale/missing get a bounded foreground fetch;
    # everything else is refreshed in the background so the page renders
    # immediately with whatever data we already have.
    all_ids = list(
        {r["item_id"] for r in cached_raw} | set(active_ids) | set(queued_ids)
    )
    needs_fetch = [iid for iid in all_ids if not _is_meta_fresh(iid)]
    has_stale   = [iid for iid in needs_fetch if iid in _meta_cache]
    truly_new   = [iid for iid in needs_fetch if iid not in _meta_cache]

    # Fetch truly-new items with a short timeout so we don't block the page
    if truly_new:
        try:
            await asyncio.wait_for(
                asyncio.gather(*[_fetch_meta(iid) for iid in truly_new]),
                timeout=3.0,
            )
        except asyncio.TimeoutError:
            pass

    # Refresh stale items in the background (don't block the response)
    if has_stale or truly_new:
        asyncio.create_task(_refresh_meta_batch(has_stale + truly_new))

    # Active sessions + watcher mapping fetched concurrently
    sessions_raw, watchers_map = await asyncio.gather(
        _fetch_sessions(),
        _fetch_watchers(),
    )
    sessions = []
    for s in sessions_raw:
        np = s.get("NowPlayingItem")
        if not np:
            continue
        ps = s.get("PlayState", {})
        pos = ps.get("PositionTicks", 0)
        run = np.get("RunTimeTicks") or 0
        iid = np.get("Id", "")
        sessions.append({
            "user": s.get("UserName", "Unknown"),
            "item_id": iid,
            "label": _item_label(np),
            "type": np.get("Type", ""),
            "is_paused": ps.get("IsPaused", False),
            "progress_pct": round(pos / run * 100, 1) if run else 0,
            "position": _fmt_runtime(pos),
            "runtime": _fmt_runtime(run),
            "client": s.get("Client", ""),
            "device": s.get("DeviceName", ""),
            "is_cached": bool(iid and (cfg.cache_dir / iid / ".ready").exists()),
            "poster_url": _poster_url(iid) if iid else None,
        })

    # Actively downloading items
    downloading = []
    for iid in active_ids:
        meta = _meta_for(iid)
        prog = progress_map.get(iid, {})
        dl = prog.get("downloaded", 0)
        tot = prog.get("total", 0)
        downloading.append({
            "item_id": iid,
            "label": _item_label(meta) if meta else iid,
            "type": meta.get("Type", ""),
            "year": meta.get("ProductionYear"),
            "downloaded_mb": round(dl / 1024 ** 2, 1),
            "total_mb": round(tot / 1024 ** 2, 1) if tot else None,
            "pct": round(dl / tot * 100, 1) if tot else 0,
            "poster_url": _poster_url(iid, meta.get("ImageTags", {}).get("Primary")),
            "watchers": watchers_map.get(iid, []),
        })

    # Items waiting for the download semaphore
    queued = []
    for iid in queued_ids:
        meta = _meta_for(iid)
        queued.append({
            "item_id": iid,
            "label": _item_label(meta) if meta else iid,
            "type": meta.get("Type", ""),
            "year": meta.get("ProductionYear"),
            "size_bytes": _item_size_bytes(meta),
            "poster_url": _poster_url(iid, meta.get("ImageTags", {}).get("Primary")),
            "watchers": watchers_map.get(iid, []),
        })

    # Fully cached library
    cached = []
    for r in cached_raw:
        meta = _meta_for(r["item_id"])
        ud = meta.get("UserData", {})
        played = ud.get("Played", False)
        lp_str = ud.get("LastPlayedDate")
        evict_at = None
        hours_until_evict = None
        if played and lp_str and cfg.evict_after_watched_hours > 0:
            try:
                lp = datetime.fromisoformat(lp_str.replace("Z", "+00:00"))
                evict_dt = lp + timedelta(hours=cfg.evict_after_watched_hours)
                hours_until_evict = (evict_dt - now).total_seconds() / 3600
                evict_at = evict_dt.strftime("%Y-%m-%d %H:%M UTC")
            except ValueError:
                pass
        tags = meta.get("ImageTags", {})
        cached.append({
            "item_id": r["item_id"],
            "size_bytes": r["size_bytes"],
            "size_gb": round(r["size_bytes"] / 1024 ** 3, 2),
            "label": _item_label(meta) if meta else r["filename"],
            "type": meta.get("Type", ""),
            "year": meta.get("ProductionYear"),
            "rating": meta.get("CommunityRating"),
            "official_rating": meta.get("OfficialRating", ""),
            "overview": (meta.get("Overview") or "")[:300],
            "runtime": _fmt_runtime(meta.get("RunTimeTicks")),
            "genres": [g["Name"] for g in meta.get("GenreItems", [])[:5]],
            "played": played,
            "play_count": ud.get("PlayCount", 0),
            "played_pct": round(ud.get("PlayedPercentage") or 0, 1),
            "evict_at": evict_at,
            "hours_until_evict": round(hours_until_evict, 1) if hours_until_evict is not None else None,
            "poster_url": _poster_url(r["item_id"], tags.get("Primary")),
            "watchers": watchers_map.get(r["item_id"], []),
        })
    # Unwatched first, then alphabetical
    cached.sort(key=lambda x: (x["played"], x["label"].lower()))

    # Pending space: queued items + remainder of in-progress downloads
    pending_bytes = sum(q.get("size_bytes", 0) for q in queued)
    for iid in active_ids:
        prog = progress_map.get(iid, {})
        remaining = max(prog.get("total", 0) - prog.get("downloaded", 0), 0)
        pending_bytes += remaining

    # Disk bar widths (pre-computed to keep template simple)
    if disk_total:
        cache_bar = min(round(cache_bytes / disk_total * 100, 1), 100)
        other_used = max(disk_used - cache_bytes, 0)
        other_bar = min(round(other_used / disk_total * 100, 1), 100 - cache_bar)
        pending_bar = min(round(pending_bytes / disk_total * 100, 1), max(0, 100 - cache_bar - other_bar))
    else:
        cache_bar = other_bar = pending_bar = 0

    return {
        "sessions": sessions,
        "downloading": downloading,
        "queued": queued,
        "cached": cached,
        "disk": {
            "total_gb": round(disk_total / 1024 ** 3, 1),
            "free_gb": round(disk_free / 1024 ** 3, 1),
            "used_gb": round(disk_used / 1024 ** 3, 1),
            "cache_gb": round(cache_bytes / 1024 ** 3, 2),
            "cache_pct": round(cache_bytes / disk_total * 100, 1) if disk_total else 0,
            "cache_bar": cache_bar,
            "other_bar": other_bar,
            "pending_bar": pending_bar,
            "pending_gb": round(pending_bytes / 1024 ** 3, 2),
        },
        "config": {
            "evict_hours": cfg.evict_after_watched_hours,
            "max_gb": cfg.cache_max_gb,
        },
        "generated_at": now.strftime("%H:%M:%S UTC"),
    }


async def _get_data() -> dict:
    global _data_cache
    t = time.monotonic()
    if _data_cache and t - _data_cache[0] < _DATA_TTL:
        return _data_cache[1]
    data = await _build_data()
    _data_cache = (t, data)
    return data


# ── Routes ─────────────────────────────────────────────────────────────────────

@router.get("/proxy/panel", response_class=HTMLResponse)
async def panel_page():
    data = await _get_data()
    tmpl = _get_env().get_template("panel.html")
    return HTMLResponse(tmpl.render(data=data, fragment=False))


@router.get("/proxy/panel/fragment", response_class=HTMLResponse)
async def panel_fragment():
    """Inner content only — fetched by the JS auto-refresh every 15 s."""
    global _data_cache
    _data_cache = None  # force a fresh fetch on every explicit refresh call
    data = await _get_data()
    tmpl = _get_env().get_template("panel.html")
    return HTMLResponse(tmpl.render(data=data, fragment=True))


@router.get("/proxy/panel/speeds")
async def panel_speeds():
    """Per-download speed history for the graph. Polled every 5 s by the dashboard JS."""
    snap = _dm.queue_snapshot()
    result = {}
    for iid in snap["active"]:
        history: list[tuple[float, int]] = snap["history"].get(iid, [])
        meta = _meta_for(iid)

        # Derive per-sample speed (MB/s) from consecutive (time, bytes) pairs
        speeds = []
        for i in range(1, len(history)):
            t1, b1 = history[i - 1]
            t2, b2 = history[i]
            dt = t2 - t1
            if dt > 0:
                speeds.append({
                    "t": int(t2 * 1000),  # Unix ms for JS Date
                    "s": round(max((b2 - b1) / dt / 1024 ** 2, 0), 3),
                })

        # Current speed: last interval
        current_mbps = 0.0
        if len(history) >= 2:
            t1, b1 = history[-2]
            t2, b2 = history[-1]
            dt = t2 - t1
            if dt > 0:
                current_mbps = round(max((b2 - b1) / dt / 1024 ** 2, 0), 2)

        result[iid] = {
            "label": _item_label(meta) if meta else iid,
            "speeds": speeds,
            "current_mbps": current_mbps,
        }
    return result

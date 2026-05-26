"""
Jellyfin Caching Proxy
======================
Sits between your local TV and the remote Jellyfin server.
- Transparently proxies all API/UI traffic to the remote server
- Intercepts media stream requests and serves from local cache when available
- Triggers background downloads when media is not cached
- All state (watched, progress, users) lives on the remote server
"""

import asyncio
import gzip
import json
import logging
import re
import shutil
import zlib
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import httpx
from fastapi import FastAPI, Request, Response, WebSocket
from fastapi.responses import FileResponse, StreamingResponse
from websockets.exceptions import ConnectionClosed
from websockets.legacy.client import connect as ws_connect

from local_hls import LocalHlsManager
import panel
from config import Config
from downloader import DownloadManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("proxy")

app = FastAPI()
cfg = Config.load()
download_manager = DownloadManager(cfg)
local_hls_manager = LocalHlsManager(
    root_dir=cfg.cache_dir / ".hls_sessions",
    ttl_seconds=cfg.local_hls_session_ttl_seconds,
    use_nvidia=cfg.local_hls_use_nvidia,
)


@app.on_event("shutdown")
async def _shutdown_local_hls_sessions():
    await local_hls_manager.stop_all()

# Regex patterns that identify media stream endpoints
LOCAL_STREAM_PATTERNS = [
    re.compile(r"^/Videos/([^/]+)/stream", re.IGNORECASE),
    re.compile(r"^/Audio/([^/]+)/stream", re.IGNORECASE),
    re.compile(r"^/Items/([^/]+)/Download", re.IGNORECASE),
]

STREAM_PATTERNS = [
    *LOCAL_STREAM_PATTERNS,
    re.compile(r"^/Videos/([^/]+)/master\.m3u8", re.IGNORECASE),
    re.compile(r"^/Videos/([^/]+)/main\.m3u8", re.IGNORECASE),
    re.compile(r"^/Videos/([^/]+)/hls\d+/.+", re.IGNORECASE),
]

# Script tag appended to every proxied HTML page
_INJECT_TAG = b'<script src="/proxy/inject.js" defer></script>'

# JavaScript injected into every Jellyfin HTML page.
# Polls /proxy/inject/status, overlays card badges, and injects a detail-page
# status line near the overview — survives SPA navigation via MutationObserver.
_INJECT_JS = r"""
(function () {
  'use strict';

  const POLL_MS = 30000;
  let _st = {}; // { itemId: { status, pct } }
  let _np = null; // most recently active local HLS session object, or null

  /* ── Styles ──────────────────────────────────────────────────────────────── */
  function injectStyles() {
    if (document.getElementById('jfc-css')) return;
    const s = document.createElement('style');
    s.id = 'jfc-css';
    s.textContent = `
      .jfc-badge {
        position: absolute; top: 5px; right: 5px;
        display: flex; align-items: center; gap: 3px;
        padding: 2px 7px; border-radius: 10px;
        font-size: 10px; font-weight: 700; line-height: 16px;
        pointer-events: none; user-select: none; z-index: 100;
        white-space: nowrap; box-shadow: 0 1px 5px rgba(0,0,0,.65);
        color: #fff;
      }
      .jfc-ready       { background: #15803d; }
      .jfc-downloading { background: #1d4ed8; }
      .jfc-queued      { background: #b45309; }

      .jfc-detail {
        display: inline-flex; align-items: center; gap: 7px;
        padding: 5px 16px; border-radius: 999px;
        font-size: 14px; font-weight: 600;
        margin-bottom: 14px; pointer-events: none;
      }
      .jfc-detail.jfc-ready {
        background: rgba(21,128,61,.18); color: #4ade80;
        border: 1px solid rgba(21,128,61,.4);
      }
      .jfc-detail.jfc-downloading {
        background: rgba(29,78,216,.18); color: #60a5fa;
        border: 1px solid rgba(29,78,216,.4);
      }
      .jfc-detail.jfc-queued {
        background: rgba(180,83,9,.18); color: #fbbf24;
        border: 1px solid rgba(180,83,9,.4);
      }

      .jfc-play-badge {
        display: inline-flex; align-items: center;
        padding: 2px 10px; border-radius: 999px;
        font-size: 11px; font-weight: 700; line-height: 18px;
        margin-left: 10px; vertical-align: middle;
        pointer-events: none; user-select: none; white-space: nowrap;
      }
      .jfc-play-local  { background: rgba(21,128,61,.25); color: #4ade80; border: 1px solid rgba(21,128,61,.5); }
      .jfc-play-remote { background: rgba(99,102,241,.25); color: #a5b4fc; border: 1px solid rgba(99,102,241,.5); }
    `;
    document.head.appendChild(s);
  }

  /* ── Status fetch ────────────────────────────────────────────────────────── */
  async function fetchPlaybackStatus() {
    try {
      const r = await fetch('/proxy/hls/sessions');
      if (!r.ok) return;
      const d = await r.json();
      const alive = (d.sessions || []).filter(s => s.alive);
      if (alive.length > 0) {
        alive.sort((a, b) => a.last_access_seconds_ago - b.last_access_seconds_ago);
        _np = alive[0];
      } else {
        _np = null;
      }
    } catch (_) {}
  }

  async function fetchStatus() {
    try {
      const r = await fetch('/proxy/inject/status');
      if (r.ok) _st = await r.json();
    } catch (_) {}
    await fetchPlaybackStatus();
    applyAll();
  }

  /* ── Text helpers ────────────────────────────────────────────────────────── */
  function cls(status) {
    return { ready: 'jfc-ready', downloading: 'jfc-downloading', queued: 'jfc-queued' }[status] || '';
  }
  function cardText(s) {
    if (s.status === 'ready')       return '✓ Cached';
    if (s.status === 'downloading') return '↓ ' + s.pct + '%';
    if (s.status === 'queued')      return '⏱ Queued';
    return '';
  }
  function detailText(s) {
    if (s.status === 'ready')       return '✓ Cached locally';
    if (s.status === 'downloading') return '↓ Downloading — ' + s.pct + '%';
    if (s.status === 'queued')      return '⏱ Queued for download';
    return '';
  }

  /* ── Extract item ID from a DOM element ──────────────────────────────────── */
  function itemIdFromEl(el) {
    // Walk up the tree looking for data-id / data-itemid
    let cur = el;
    for (let i = 0; i < 12; i++) {
      if (!cur) break;
      const id = cur.dataset.id || cur.dataset.itemid;
      if (id) return id;
      cur = cur.parentElement;
    }
    // Extract from CSS background-image URL
    const bg = el.style && el.style.backgroundImage || '';
    const m1 = bg.match(/\/Items\/([^/?]+)\/Images/);
    if (m1) return m1[1];
    // Extract from img src
    const src = el.getAttribute && el.getAttribute('src') || '';
    const m2 = src.match(/\/Items\/([^/?]+)\/Images/);
    if (m2) return m2[1];
    return null;
  }

  /* ── Card badges ─────────────────────────────────────────────────────────── */
  function applyBadgeToContainer(container, itemId) {
    // Remove stale badge
    container.querySelectorAll('.jfc-badge').forEach(b => b.remove());

    const s = _st[itemId];
    if (!s) return;

    if (getComputedStyle(container).position === 'static') {
      container.style.position = 'relative';
    }
    const badge = document.createElement('div');
    badge.className = 'jfc-badge ' + cls(s.status);
    badge.title = detailText(s);
    badge.textContent = cardText(s);
    container.appendChild(badge);
  }

  function applyCardBadges() {
    // Cards with data-id (Jellyfin sets this on .card and the inner button)
    document.querySelectorAll('[data-id]').forEach(el => {
      const itemId = el.dataset.id;
      if (!itemId || !_st[itemId]) {
        el.querySelectorAll('.jfc-badge').forEach(b => b.remove());
        return;
      }
      const container = el.querySelector('.cardImageContainer') || el.querySelector('[class*="cardImage"]');
      if (container) applyBadgeToContainer(container, itemId);
    });

    // Fallback: CSS background-image cards that may lack data-id
    document.querySelectorAll('[style*="/Items/"][style*="/Images/"]').forEach(el => {
      const itemId = itemIdFromEl(el);
      if (!itemId) return;
      const container = el.closest('.cardImageContainer') || el.parentElement;
      if (!container) return;
      if (!container.querySelector('.jfc-badge') || _st[itemId]) {
        applyBadgeToContainer(container, itemId);
      }
    });

    // Fallback: <img> based cards
    document.querySelectorAll('img[src*="/Items/"][src*="/Images/"]').forEach(img => {
      const itemId = itemIdFromEl(img);
      if (!itemId) return;
      const container = img.closest('.cardImageContainer') || img.parentElement;
      if (!container) return;
      if (!container.querySelector('.jfc-badge') || _st[itemId]) {
        applyBadgeToContainer(container, itemId);
      }
    });
  }

  /* ── Detail page badge ───────────────────────────────────────────────────── */
  function applyDetailBadge() {
    document.querySelectorAll('.jfc-detail').forEach(el => el.remove());

    const m = location.hash.match(/[?&]id=([^&]+)/);
    if (!m) return;
    const s = _st[m[1]];
    if (!s) return;

    const badge = document.createElement('div');
    badge.className = 'jfc-detail ' + cls(s.status);
    badge.textContent = detailText(s);

    // Try to insert before the overview or near the buttons row
    const anchors = [
      '.itemDetailPage .overview',
      '.itemDetailPage [class*="overview"]',
      '.detailPageContent .overview',
      '.detailSectionContent .overview',
      '.itemDetailPage .detailButtons',
      '.itemDetailPage .itemButtons',
      '.itemDetailPage .buttons',
      '.itemDetailPage h2',
      '.itemDetailPage h1',
    ];
    for (const sel of anchors) {
      const target = document.querySelector(sel);
      if (target && target.parentElement) {
        target.parentElement.insertBefore(badge, target);
        return;
      }
    }
    // Last resort: fixed overlay in the corner
    badge.style.cssText = 'position:fixed;bottom:72px;right:16px;z-index:9999;pointer-events:none';
    document.body.appendChild(badge);
  }

  /* ── Playback source badge ───────────────────────────────────────────────── */
  function applyPlaybackBadge() {
    document.querySelectorAll('.jfc-play-badge').forEach(el => el.remove());

    // Only active when a video element is on the page
    if (!document.querySelector('video')) return;

    // Item ID: active HLS session → URL hash → <video src>
    let itemId = _np ? _np.item_id : null;
    if (!itemId) {
      const mHash = location.hash.match(/[?&]id=([a-f0-9-]+)/i);
      if (mHash) itemId = mHash[1].replace(/-/g, '');
    }
    if (!itemId) {
      const vid = document.querySelector('video');
      if (vid && vid.src) {
        const mSrc = vid.src.match(/\/[Vv]ideos\/([a-f0-9-]+)\//);
        if (mSrc) itemId = mSrc[1].replace(/-/g, '');
      }
    }

    // Nothing identifiable — skip
    if (!_np && !itemId) return;

    // Local = active HLS session (always from cache) OR item has "ready" status
    const isLocal = !!_np || !!(itemId && _st[itemId] && _st[itemId].status === 'ready');

    const badge = document.createElement('span');
    badge.className = 'jfc-play-badge ' + (isLocal ? 'jfc-play-local' : 'jfc-play-remote');
    badge.textContent = isLocal ? '⬇ Local' : '☁ Remote';

    // pageTitle is the header <h3> always visible during playback;
    // OSD selectors are secondary (only present while the overlay is shown)
    const titleSelectors = [
      '.pageTitle',
      '.mediaInfoPrimary',
      '.videoOsdBottom .mediaInfoItem',
      '.osdMediaInfo',
      '.nowPlayingInfo',
    ];
    for (const sel of titleSelectors) {
      const el = document.querySelector(sel);
      if (el) { el.appendChild(badge); return; }
    }

    // Fallback: fixed overlay above the controls
    badge.style.cssText += 'position:fixed;bottom:80px;left:20px;z-index:9999;';
    document.body.appendChild(badge);
  }

  /* ── Combined ────────────────────────────────────────────────────────────── */
  function applyAll() {
    applyCardBadges();
    applyDetailBadge();
    applyPlaybackBadge();
  }

  /* ── Boot ────────────────────────────────────────────────────────────────── */
  injectStyles();
  fetchStatus();
  setInterval(fetchStatus, POLL_MS);

  // SPA hash navigation
  window.addEventListener('hashchange', applyAll);

  // Debounced MutationObserver — fires when Jellyfin renders new cards
  let _timer;
  new MutationObserver(() => {
    clearTimeout(_timer);
    _timer = setTimeout(applyAll, 200);
  }).observe(document.body, { childList: true, subtree: true });
})();
""".encode()


def extract_item_id(path: str) -> str | None:
    for pattern in STREAM_PATTERNS:
        m = pattern.match(path)
        if m:
            return normalize_item_id(m.group(1))
    return None


def normalize_item_id(item_id: str) -> str:
    return item_id.replace("-", "")


def can_serve_local_file(path: str) -> bool:
    return any(pattern.match(path) for pattern in LOCAL_STREAM_PATTERNS)


def is_hls_segment_path(path: str) -> bool:
    return bool(re.match(r"^/Videos/[^/]+/hls\d+/.+\.(mp4|m4s|ts)$", path, re.IGNORECASE))


def is_hls_playlist_path(path: str) -> bool:
    return bool(re.match(r"^/Videos/[^/]+/(master|main)\.m3u8$", path, re.IGNORECASE))


def _remote_ws_url_from_client(websocket: WebSocket) -> str:
    incoming = urlsplit(str(websocket.url))
    remote = urlsplit(cfg.remote_url)
    ws_scheme = "wss" if remote.scheme == "https" else "ws"
    return urlunsplit((ws_scheme, remote.netloc, incoming.path, incoming.query, ""))


def local_ready_path(item_id: str) -> Path | None:
    """Return the path to the locally cached file if it's fully downloaded, else None.

    Jellyfin uses two ID formats interchangeably:
    - UUID with dashes:    bb72d2e9-2050-32e6-804c-ab4915f0ffc7  (used in URL paths)
    - Hex without dashes:  bb72d2e9205032e6804cab4915f0ffc7      (used in API / cache dirs)
    We try both so a cache hit is found regardless of which form the caller supplies.
    """
    canonical = normalize_item_id(item_id)
    candidates = [item_id, canonical]
    for cid in dict.fromkeys(candidates):  # deduplicate while preserving order
        item_dir = cfg.cache_dir / cid
        marker = item_dir / ".ready"

        if marker.exists():
            media_file = item_dir / marker.read_text().strip()
            if media_file.exists() and media_file.stat().st_size > 0:
                return media_file

        # Self-heal older caches or interrupted migrations where the media file
        # exists but the .ready marker has not been written yet.
        media_files = [
            p for p in item_dir.iterdir()
            if p.is_file() and not p.name.startswith(".") and p.suffix.lower() in {".mkv", ".mp4", ".mov", ".m4v", ".ts", ".m2ts", ".webm"}
        ] if item_dir.exists() else []
        if len(media_files) == 1 and media_files[0].stat().st_size > 0:
            try:
                marker.write_text(media_files[0].name)
            except Exception:
                pass
            return media_files[0]
    return None


def _decompress(content: bytes, encoding: str) -> bytes:
    """Decompress content if the encoding is recognised. Returns content unchanged on failure."""
    enc = encoding.lower().strip()
    try:
        if enc == "br":
            import brotli  # optional dep; installed in venv
            return brotli.decompress(content)
        if enc == "gzip":
            return gzip.decompress(content)
        if enc in ("deflate", "zlib"):
            return zlib.decompress(content)
    except Exception:
        pass
    return content


def _mp4_split_at_first_moof(data: bytes) -> tuple[bytes, bytes]:
    """Split raw fMP4 bytes into (init_boxes, media_boxes).

    init_boxes = everything before the first moof/mdat (i.e. ftyp + moov).
    media_boxes = the moof/mdat remainder.
    """
    i = 0
    while i + 8 <= len(data):
        size = int.from_bytes(data[i : i + 4], "big")
        if size == 1:  # 64-bit extended size
            if i + 16 > len(data):
                break
            size = int.from_bytes(data[i + 8 : i + 16], "big")
        if size < 8:
            break
        box_type = data[i + 4 : i + 8]
        if box_type in (b"moof", b"mdat"):
            return data[:i], data[i:]
        i += size
    return data, b""


async def serve_hls_segment_local(local_file: Path, request: Request) -> Response:
    """Serve an HLS fMP4 segment from a locally cached raw media file via FFmpeg.

    The init segment (-N.mp4) gets the ftyp+moov boxes.
    Regular segments get the moof+mdat boxes starting at runtimeTicks.
    Falls back to stream_remote() on any FFmpeg error.
    """
    query = dict(request.query_params)
    runtime_ticks = int(query.get("runtimeTicks", 0))
    segment_ticks = int(query.get("actualSegmentLengthTicks", 0))

    # Segment names: "-1" is the init segment; positive numbers are media segments.
    stem = Path(request.url.path).stem  # e.g. "-1", "630"
    is_init = stem.startswith("-")

    start_s = runtime_ticks / 10_000_000
    duration_s = segment_ticks / 10_000_000 if segment_ticks > 0 else 6.0

    base_ffmpeg = [
        "ffmpeg", "-y", "-loglevel", "error",
    ]
    fmp4_flags = "frag_keyframe+empty_moov+default_base_moof"

    if is_init:
        # Produce a short fMP4 stream and keep only the init boxes (ftyp+moov).
        cmd = [
            *base_ffmpeg,
            "-i", str(local_file),
            "-t", "0.5",
            "-c:v", "copy", "-c:a", "aac",
            "-movflags", fmp4_flags,
            "-f", "mp4", "pipe:1",
        ]
    else:
        # Seek to start_s in the input before decoding (fast keyframe seek).
        cmd = [
            *base_ffmpeg,
            "-ss", str(start_s),
            "-i", str(local_file),
            "-t", str(duration_s),
            "-c:v", "copy", "-c:a", "aac", "-b:a", "384k",
            "-movflags", fmp4_flags,
            "-f", "mp4", "pipe:1",
        ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0 or not stdout:
        log.error(
            "Local FFmpeg failed for %s (rc=%d): %s",
            request.url.path, proc.returncode, stderr.decode()[-500:],
        )
        return await stream_remote(request)

    init_data, media_data = _mp4_split_at_first_moof(stdout)

    if is_init:
        payload = init_data or stdout  # fall back to full output if parsing failed
    else:
        payload = media_data or stdout

    if not payload:
        log.error("Local FFmpeg produced empty payload for %s", request.url.path)
        return await stream_remote(request)

    log.info("LOCAL FFMPEG [%s] %s", "init" if is_init else f"{start_s:.1f}s", request.url.path)
    return Response(
        content=payload,
        media_type="video/mp4",
        headers={"Cache-Control": "no-store", "X-Cache": "LOCAL-FFMPEG"},
    )


def _inject_into_html(content: bytes, resp_headers: dict) -> bytes:
    """Inject the script tag into an already-decoded HTML body."""
    for marker in (b"</body>", b"</html>"):
        if marker in content:
            return content.replace(marker, _INJECT_TAG + marker, 1)
    return content + _INJECT_TAG


async def proxy_request(request: Request, override_url: str | None = None) -> Response:
    """Forward a request to the remote Jellyfin server and return its response."""
    url = override_url or str(request.url).replace(
        str(request.base_url).rstrip("/"), cfg.remote_url
    )

    headers = dict(request.headers)
    # Strip headers that would confuse the upstream or cause encoding problems.
    # Removing accept-encoding tells the upstream to send plain (uncompressed)
    # bytes, avoiding any mismatch between what httpx decodes and what the
    # original Content-Encoding header claims.  We still decompress manually
    # below as a fallback for upstreams that ignore this.
    for h in ("host", "content-length", "accept-encoding"):
        headers.pop(h, None)

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.request(
            method=request.method,
            url=url,
            headers=headers,
            content=await request.body(),
            follow_redirects=False,
        )

    resp_headers = dict(resp.headers)

    # Rewrite absolute Location headers so the browser stays on the proxy
    # rather than being redirected directly to the remote Jellyfin origin.
    location = resp_headers.get("location", "")
    if location.startswith(cfg.remote_url):
        resp_headers["location"] = location[len(cfg.remote_url):]

    # 304 / 204 carry no body — pass through untouched so the browser's
    # conditional-request / cache machinery works correctly.
    if resp.status_code in (204, 304):
        return Response(content=resp.content, status_code=resp.status_code, headers=resp_headers)

    # Decode the body.  httpx auto-decompresses in most cases, but some
    # upstream CDN configurations return compressed bytes even when we didn't
    # request encoding.  Decompress explicitly, then strip the stale headers.
    encoding = resp_headers.pop("content-encoding", "")
    resp_headers.pop("content-length", None)   # Starlette recalculates this
    content = _decompress(resp.content, encoding)

    if "text/html" in resp_headers.get("content-type", ""):
        content = _inject_into_html(content, resp_headers)

    return Response(
        content=content,
        status_code=resp.status_code,
        headers=resp_headers,
    )


async def stream_remote(request: Request) -> StreamingResponse:
    """Proxy a media stream from the remote server, streaming the response."""
    url = str(request.url).replace(
        str(request.base_url).rstrip("/"), cfg.remote_url
    )

    headers = dict(request.headers)
    req_body = await request.body()
    hls_segment = is_hls_segment_path(request.url.path)

    for h in ("host",):
        headers.pop(h, None)

    if hls_segment:
        # Conditional requests can lead to 304 for media segments, which breaks
        # some clients during HLS playback. Always request the segment body.
        for h in (
            "if-none-match",
            "if-modified-since",
            "if-match",
            "if-unmodified-since",
            "if-range",
        ):
            headers.pop(h, None)
        headers["cache-control"] = "no-cache"
        headers["pragma"] = "no-cache"

    # For HLS segments the remote Jellyfin transcoder may not have reached the
    # requested position yet and returns 502/503/504 until it catches up.
    # Retry transparently so Firefox never sees the error and doesn't start its
    # own disruptive retry storm.
    _HLS_RETRY_STATUSES = (502, 503, 504)
    _HLS_MAX_RETRIES = 20
    _HLS_RETRY_DELAY = 3.0

    client: httpx.AsyncClient | None = None
    resp: httpx.Response | None = None
    for attempt in range(_HLS_MAX_RETRIES if hls_segment else 1):
        if client is not None:
            await resp.aclose()
            await client.aclose()
        client = httpx.AsyncClient(timeout=None)
        req = client.build_request(
            method=request.method,
            url=url,
            headers=headers,
            content=req_body,
        )
        resp = await client.send(req, stream=True, follow_redirects=True)
        if not (hls_segment and resp.status_code in _HLS_RETRY_STATUSES):
            break
        logging.info(
            "HLS segment %s returned %d (attempt %d/%d), retrying in %.0fs",
            request.url.path, resp.status_code, attempt + 1, _HLS_MAX_RETRIES, _HLS_RETRY_DELAY,
        )
        await asyncio.sleep(_HLS_RETRY_DELAY)

    async def _gen():
        try:
            # Forward raw bytes to preserve upstream encoding and avoid
            # content-encoding/content-length mismatches.
            async for chunk in resp.aiter_raw(chunk_size=65536):
                yield chunk
        finally:
            await resp.aclose()
            await client.aclose()

    out_headers = {
        k: v
        for k, v in resp.headers.items()
        if k.lower()
        not in (
            "connection",
            "keep-alive",
            "proxy-connection",
            "upgrade",
            "transfer-encoding",
            "content-length",
        )
    }

    if hls_segment:
        # Prevent clients from revalidating media segments as cache objects.
        for h in ("etag", "last-modified", "expires", "age"):
            out_headers.pop(h, None)
        out_headers["cache-control"] = "no-store, no-cache, must-revalidate"
        out_headers["pragma"] = "no-cache"

    return StreamingResponse(
        _gen(),
        status_code=resp.status_code,
        headers=out_headers,
        media_type=resp.headers.get("content-type", "video/mp4"),
    )


@app.websocket("/socket")
@app.websocket("/socket/{subpath:path}")
async def websocket_proxy(websocket: WebSocket, subpath: str = ""):
    remote_ws_url = _remote_ws_url_from_client(websocket)
    forward_headers = {
        k: v
        for k, v in websocket.headers.items()
        if k.lower() not in ("host", "connection", "upgrade", "sec-websocket-key", "sec-websocket-version", "sec-websocket-extensions", "sec-websocket-protocol")
    }
    requested_subprotocols = [p.strip() for p in websocket.headers.get("sec-websocket-protocol", "").split(",") if p.strip()]

    try:
        async with ws_connect(
            remote_ws_url,
            extra_headers=forward_headers,
            subprotocols=requested_subprotocols or None,
            open_timeout=30,
            ping_interval=20,
            ping_timeout=20,
            max_size=None,
        ) as upstream:
            await websocket.accept(subprotocol=upstream.subprotocol)

            async def client_to_upstream():
                while True:
                    message = await websocket.receive()
                    msg_type = message.get("type")
                    if msg_type == "websocket.disconnect":
                        await upstream.close()
                        return
                    text = message.get("text")
                    if text is not None:
                        await upstream.send(text)
                        continue
                    data = message.get("bytes")
                    if data is not None:
                        await upstream.send(data)

            async def upstream_to_client():
                while True:
                    payload = await upstream.recv()
                    if isinstance(payload, bytes):
                        await websocket.send_bytes(payload)
                    else:
                        await websocket.send_text(payload)

            relay_tasks = [
                asyncio.create_task(client_to_upstream()),
                asyncio.create_task(upstream_to_client()),
            ]
            done, pending = await asyncio.wait(relay_tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                exc = task.exception()
                if not exc:
                    continue
                if isinstance(exc, ConnectionClosed):
                    continue
                if isinstance(exc, RuntimeError) and "Unexpected ASGI message 'websocket.send'" in str(exc):
                    # Peer disconnected while relay task was still flushing a message.
                    continue
                raise exc
    except ConnectionClosed:
        if websocket.client_state.name == "CONNECTED":
            await websocket.close()
    except Exception:
        log.exception("WebSocket proxy error for %s", remote_ws_url)
        try:
            await websocket.close(code=1011)
        except Exception:
            pass


# ── Proxy-specific routes ─────────────────────────────────────────────────────
# ALL explicit routes must be registered before the catch-all /{path:path},
# otherwise the catch-all matches first and proxies them to the remote server.

# Panel routes are registered here too (before the catch-all).
panel.setup(cfg, download_manager)
app.include_router(panel.router)


@app.get("/proxy/inject.js")
async def inject_js():
    """Client-side script injected into every Jellyfin HTML page."""
    return Response(
        content=_INJECT_JS,
        media_type="application/javascript",
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/proxy/inject/status")
async def inject_status():
    """Cache status for all known items — consumed by the injected JS."""
    snap = download_manager.queue_snapshot()
    items: dict[str, dict] = {}

    if cfg.cache_dir.exists():
        for item_dir in cfg.cache_dir.iterdir():
            if not item_dir.is_dir():
                continue
            if (item_dir / ".ready").exists():
                items[item_dir.name] = {"status": "ready"}

    for iid in snap["active"]:
        prog = snap["progress"].get(iid, {})
        dl = prog.get("downloaded", 0)
        tot = prog.get("total", 0)
        items[iid] = {
            "status": "downloading",
            "pct": round(dl / tot * 100, 1) if tot else 0,
        }

    for iid in snap["queued"]:
        if iid not in items:
            items[iid] = {"status": "queued"}

    return items


@app.get("/proxy/cache/status")
async def cache_status():
    """JSON snapshot of what's cached locally."""
    items = {}
    if cfg.cache_dir.exists():
        for item_dir in cfg.cache_dir.iterdir():
            if not item_dir.is_dir():
                continue
            marker = item_dir / ".ready"
            in_progress = item_dir / ".downloading"
            if marker.exists():
                fname = marker.read_text().strip()
                fpath = item_dir / fname
                items[item_dir.name] = {
                    "status": "ready",
                    "file": fname,
                    "size_mb": round(fpath.stat().st_size / 1024 / 1024, 1) if fpath.exists() else 0,
                }
            elif in_progress.exists():
                items[item_dir.name] = {"status": "downloading"}

    return {
        "cached_items": len([v for v in items.values() if v["status"] == "ready"]),
        "downloading": len([v for v in items.values() if v["status"] == "downloading"]),
        "items": items,
    }


@app.get("/proxy/hls/session/{session_id}/master.m3u8")
async def local_hls_master_playlist(session_id: str, request: Request):
    playlist = await local_hls_manager.get_rewritten_master_playlist(
        session_id=session_id,
        base_url=str(request.base_url),
    )
    return Response(
        content=playlist,
        media_type="application/vnd.apple.mpegurl",
        headers={"Cache-Control": "no-store", "X-Cache": "LOCAL-HLS-PLAYLIST"},
    )


@app.get("/proxy/hls/session/{session_id}/variant/{variant}/playlist.m3u8")
async def local_hls_variant_playlist(session_id: str, variant: int, request: Request):
    playlist = await local_hls_manager.get_rewritten_variant_playlist(
        session_id=session_id,
        variant=variant,
        base_url=str(request.base_url),
    )
    return Response(
        content=playlist,
        media_type="application/vnd.apple.mpegurl",
        headers={"Cache-Control": "no-store", "X-Cache": "LOCAL-HLS-VARIANT-PLAYLIST"},
    )


@app.get("/proxy/hls/session/{session_id}/variant/{variant}/segment/{name}")
async def local_hls_segment(session_id: str, variant: int, name: str):
    seg_path = await local_hls_manager.wait_for_segment(
        session_id=session_id,
        variant=variant,
        name=name,
    )
    media_type = "video/mp4" if name.endswith((".m4s", ".mp4")) else "application/octet-stream"
    return FileResponse(
        path=seg_path,
        media_type=media_type,
        headers={"Cache-Control": "no-store", "X-Cache": "LOCAL-HLS-SEGMENT"},
    )


@app.get("/proxy/hls/sessions")
async def local_hls_sessions():
    return local_hls_manager.snapshot()


@app.delete("/proxy/cache/{item_id}")
async def evict_item(item_id: str):
    """Manually evict a cached item."""
    item_dir = cfg.cache_dir / item_id
    if item_dir.exists():
        shutil.rmtree(item_dir)
        log.info("EVICTED [%s]", item_id)
        return {"evicted": True}
    return {"evicted": False}


# ── Eviction helpers (Layer 1: proxy-intercept) ───────────────────────────────

_WATCHED_THRESHOLD = 0.85

_PLAYED_ITEMS_RE = re.compile(
    r"^/Users/[^/]+/PlayedItems/([^/]+)$", re.IGNORECASE
)


def _maybe_schedule_eviction(item_id: str) -> None:
    item_id = normalize_item_id(item_id)
    if cfg.evict_after_watched_hours <= 0:
        return
    if not (cfg.cache_dir / item_id / ".ready").exists():
        return
    log.info("WATCHED [%s] — evicting from cache in %.1fh", item_id, cfg.evict_after_watched_hours)
    asyncio.create_task(_evict_after_delay(item_id))


async def _evict_after_delay(item_id: str) -> None:
    await asyncio.sleep(cfg.evict_after_watched_hours * 3600)
    if (cfg.cache_dir / item_id / ".ready").exists():
        await download_manager.evict_item(item_id)
        log.info("EVICTED [%s] after watch window elapsed", item_id)


# ── Catch-all — MUST be last ──────────────────────────────────────────────────

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
async def catch_all(request: Request, path: str):
    req_path = "/" + path

    # Layer 1a: client reports playback stopped
    if request.method == "DELETE" and req_path.lower() == "/sessions/playing":
        try:
            data = json.loads(await request.body())
            stop_id = normalize_item_id(data.get("ItemId") or data.get("itemId", ""))
            pos = int(data.get("PositionTicks", 0))
            run = int(data.get("RunTimeTicks", 0))
            if stop_id and run and pos / run >= _WATCHED_THRESHOLD:
                _maybe_schedule_eviction(stop_id)
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    # Layer 1b: client manually marks an item as played
    if request.method == "POST":
        m = _PLAYED_ITEMS_RE.match(req_path)
        if m:
            _maybe_schedule_eviction(normalize_item_id(m.group(1)))

    item_id = extract_item_id(req_path)

    if item_id:
        if is_hls_playlist_path(req_path):
            local_file = local_ready_path(item_id)
            if local_file:
                session = await local_hls_manager.get_or_create_session(
                    item_id=item_id,
                    source_file=local_file,
                    request_query=dict(request.query_params),
                )
                if req_path.lower().endswith("/master.m3u8"):
                    playlist = await local_hls_manager.get_rewritten_master_playlist(
                        session_id=session.session_id,
                        base_url=str(request.base_url),
                    )
                    cache_hdr = "LOCAL-HLS-MASTER-PLAYLIST"
                else:
                    # Jellyfin's main.m3u8 behaves as media playlist on many clients.
                    # We serve the top rendition variant to stay compatible.
                    playlist = await local_hls_manager.get_rewritten_variant_playlist(
                        session_id=session.session_id,
                        variant=0,
                        base_url=str(request.base_url),
                    )
                    cache_hdr = "LOCAL-HLS-VARIANT-PLAYLIST"
                return Response(
                    content=playlist,
                    media_type="application/vnd.apple.mpegurl",
                    headers={"Cache-Control": "no-store", "X-Cache": cache_hdr},
                )
            if cfg.enable_local_hls_transcode:
                log.info("Local HLS requested for %s but cache is not ready", item_id)

        hls_seg = is_hls_segment_path(req_path)
        # Legacy per-segment local FFmpeg path is intentionally disabled.
        # Local HLS now uses dedicated /proxy/hls/session/* endpoints.
        if hls_seg:
            return await stream_remote(request)

        local_candidate = can_serve_local_file(req_path)
        # Check local cache both for direct-stream paths and HLS segment paths.
        local_file = local_ready_path(item_id) if (local_candidate or hls_seg) else None
        if local_file and local_candidate:
            log.info("CACHE HIT  [%s] → %s", item_id, local_file)
            return FileResponse(
                path=local_file,
                media_type="video/mp4",
                headers={"Accept-Ranges": "bytes", "X-Cache": "HIT"},
            )
        else:
            log.info("CACHE MISS [%s] → proxying to remote", item_id)
            # HLS manifests/segments can generate many requests; only trigger
            # background caching for endpoints that map to direct file serving.
            if local_candidate:
                asyncio.create_task(download_manager.enqueue(item_id))
            return await stream_remote(request)

    return await proxy_request(request)

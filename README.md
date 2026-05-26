# Jellyfin Caching Proxy

A lightweight caching proxy that sits between your local TV and your remote Jellyfin server.

## How it works

- Your TV connects to the proxy (on your LAN) instead of directly to Jellyfin
- All API calls (metadata, auth, search, watched state) pass through transparently to the remote server
- Media stream requests are intercepted:
  - If the file is **fully cached locally** → served from local disk (fast, no internet needed)
  - If **not cached** → streamed from the remote Jellyfin, and a background download is triggered
- A prefetch daemon proactively downloads your **Continue Watching** and **Next Up** items
- Watched files are automatically evicted via two complementary layers (see below)

All state (watched history, progress, users) lives on the remote Jellyfin. The proxy is stateless.

### HLS transcoding stability

For most TVs/browsers, playback is smoother when HLS transcoding stays on the
remote Jellyfin server. The proxy therefore defaults to:

```bash
JELLYFIN_ENABLE_LOCAL_HLS_TRANSCODE=0
```

Only set it to `1` if you specifically want local FFmpeg-generated HLS segments
from cached files and accept possible A/V sync issues on some clients.

When local HLS is enabled, playlists are intercepted and rewritten to proxy-owned
local HLS session endpoints. Useful knobs:

```bash
JELLYFIN_ENABLE_LOCAL_HLS_TRANSCODE=1
JELLYFIN_LOCAL_HLS_SESSION_TTL_SECONDS=300
JELLYFIN_LOCAL_HLS_USE_NVIDIA=0
```

Set `JELLYFIN_LOCAL_HLS_USE_NVIDIA=1` only if your container runtime has access
to Nvidia devices and FFmpeg supports NVENC.

With Docker Compose, use the optional Nvidia overlay:

```bash
docker compose -f compose.yaml -f compose.override.yaml -f compose.nvidia.yaml up -d --build
```

If NVENC is unavailable at runtime, local HLS sessions automatically fall back
to CPU transcoding.

## Auto-eviction after watching

Because the proxy is only reachable from the local network, the remote Jellyfin server cannot
call back into it (e.g. via webhooks). Eviction is therefore handled entirely by the proxy itself,
using two layers:

**Layer 1 — Proxy intercept (fast):** The TV client sends all its Jellyfin API calls through the
proxy on the way to the remote server. The proxy watches for two signals:

| Client call | Meaning |
|---|---|
| `DELETE /Sessions/Playing` with ≥ 85% position | Playback stopped near the end |
| `POST /Users/{id}/PlayedItems/{itemId}` | Item manually marked as played |

When either is detected for a cached item, a delayed eviction task is scheduled
(`JELLYFIN_EVICT_AFTER_WATCHED_HOURS` hours from now).

**Layer 2 — Periodic poll (fallback):** Every prefetch cycle the daemon checks every cached item's
`UserData.Played` and `LastPlayedDate` via the Jellyfin API. Any file whose eviction deadline has
passed is removed. This catches items watched on other clients, and recovers from proxy restarts
that would otherwise lose the in-flight Layer 1 task.

Set `JELLYFIN_EVICT_AFTER_WATCHED_HOURS=0` to disable eviction entirely (keep files forever).

## Setup

### 1. Install dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure

```bash
cp env.example .env
# Edit .env with your remote Jellyfin URL, API key, and user ID
```

Get your API key from: **Jellyfin Dashboard → API Keys → +**

Get your user ID from: **Jellyfin Dashboard → Users → click your user → copy the UUID from the URL**

### 3. Create the cache directory

```bash
sudo mkdir -p /mnt/jellyfin-cache
sudo chown $USER /mnt/jellyfin-cache
```

### 4. Run

```bash
python main.py
```

Or run proxy and prefetch separately:

```bash
# Proxy only
uvicorn proxy:app --host 0.0.0.0 --port 8096

# Prefetch daemon only
python prefetch.py
```

### 5. Point your TV at the proxy

In your Jellyfin client (TV), add a new server:
```
http://<local-server-ip>:8096
```

Log in with the same credentials as your remote server (auth is proxied).

### 6. (Optional) Run as a systemd service

```bash
sudo cp jellyfin-proxy.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable jellyfin-proxy
sudo systemctl start jellyfin-proxy

# View logs
journalctl -u jellyfin-proxy -f
```

### 7. Run with Docker

Build the image:

```bash
docker build -t jellyfin-cache-proxy .
```

Run the container:

```bash
docker run -d \
  --name jellyfin-cache-proxy \
  --restart unless-stopped \
  --env-file .env \
  -p 8096:8096 \
  -v /mnt/jellyfin-cache:/cache \
  jellyfin-cache-proxy
```

View logs:

```bash
docker logs -f jellyfin-cache-proxy
```

Stop/remove:

```bash
docker rm -f jellyfin-cache-proxy
```

### 8. Local development with Docker Compose

Set host UID/GID in `.env` so the container can write to the bind-mounted
cache directory:

```bash
echo "HOST_UID=$(id -u)" >> .env
echo "HOST_GID=$(id -g)" >> .env
```

Build and start:

```bash
docker compose -f compose.yaml -f compose.override.yaml up -d --build
```

Tail logs:

```bash
docker compose -f compose.yaml -f compose.override.yaml logs -f
```

Stop:

```bash
docker compose -f compose.yaml -f compose.override.yaml down
```

Note: keep `compose.override.yaml` in the command for deployments so the cache
mount points to `/media/arnyminerz/MagatzemSSD1/Jellyfin`.

## Web dashboard

A live dashboard is available at `http://<local-server-ip>:8096/proxy/panel` showing:

- Active playback sessions (user, client, progress)
- Downloads in progress (with byte-level progress bar)
- Queued downloads
- Cached library with rich metadata (poster, rating, genres, runtime, overview)
- Per-item watched status and eviction countdown
- Disk usage (cache vs. total)

The page auto-refreshes every 15 seconds.

## API endpoints

| Endpoint | Description |
|---|---|
| `GET /proxy/panel` | Web dashboard |
| `GET /proxy/cache/status` | JSON: what's cached and what's downloading |
| `GET /proxy/hls/session/{id}/master.m3u8` | Local ABR master playlist |
| `GET /proxy/hls/session/{id}/variant/{n}/playlist.m3u8` | Local ABR variant playlist |
| `GET /proxy/hls/session/{id}/variant/{n}/segment/{name}` | Local ABR segment |
| `GET /proxy/hls/sessions` | JSON: active local HLS transcode sessions |
| `DELETE /proxy/cache/{item_id}` | Manually evict a cached item |

## Cache directory layout

```
/mnt/jellyfin-cache/
  <item-id>/
    .downloading     ← lock file, present during download
    Movie.Name.mkv   ← the actual media file
    .ready           ← marker file (contains filename), written last
```

A file is only served locally when `.ready` exists. In-progress downloads
are invisible to the proxy — it falls back to remote for those.

## Availability matrix

| Situation | Result |
|---|---|
| File cached + local server up | ✅ Fast local playback |
| File not cached + internet ok | ✅ Streams from remote, download starts |
| File not cached + no internet | ❌ Cannot play |
| Local server down | TV should fall back to remote server directly |
| Remote server down + file cached | ✅ Plays fine |
| Remote server down + not cached | ❌ Cannot play |

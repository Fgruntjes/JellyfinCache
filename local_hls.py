import asyncio
import hashlib
import logging
import math
import os
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode

from fastapi import HTTPException

log = logging.getLogger("local_hls")

SEG_DURATION = 2.0          # seconds per HLS segment
SEEK_RESTART_THRESHOLD = 15  # restart ffmpeg if requested segment is >N ahead of latest produced


@dataclass(frozen=True)
class AbrRendition:
    name: str
    width: int
    height: int
    video_bitrate_k: int
    maxrate_k: int
    bufsize_k: int
    audio_bitrate_k: int


DEFAULT_ABR_RENDITIONS: tuple[AbrRendition, ...] = (
    AbrRendition("1080p", 1920, 1080, 5000, 5350, 7500, 192),
    AbrRendition("720p", 1280, 720, 2800, 3000, 4500, 160),
    AbrRendition("480p", 854, 480, 1400, 1600, 2400, 128),
)


@dataclass
class HlsSession:
    session_id: str
    item_id: str
    source_file: Path
    dir_path: Path
    master_playlist_path: Path
    started_at: float
    last_access: float
    ffmpeg_proc: asyncio.subprocess.Process
    duration: float
    total_segments: int
    start_seconds: float
    use_nvidia: bool
    ffmpeg_start_seg: int


class LocalHlsManager:
    def __init__(
        self,
        root_dir: Path,
        ttl_seconds: int = 300,
        use_nvidia: bool = False,
        renditions: tuple[AbrRendition, ...] = DEFAULT_ABR_RENDITIONS,
    ):
        self.root_dir = root_dir
        self.ttl_seconds = ttl_seconds
        self.use_nvidia = use_nvidia
        self.renditions = renditions
        self._sessions: dict[str, HlsSession] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._stderr_buffers: dict[int, bytearray] = {}

    async def get_or_create_session(
        self,
        item_id: str,
        source_file: Path,
        request_query: dict,
    ) -> HlsSession:
        await self._cleanup_stale_sessions()

        session_key = self._session_key(item_id, source_file, request_query)
        lock = self._locks.setdefault(session_key, asyncio.Lock())

        async with lock:
            sess = self._sessions.get(session_key)
            if sess and self._is_process_alive(sess.ffmpeg_proc):
                sess.last_access = time.time()
                return sess

            if sess:
                await self._stop_session(session_key)

            session_id = self._session_id(session_key)
            session_dir = self.root_dir / session_id
            master_playlist_path = session_dir / "master.m3u8"
            q_lower = {k.lower(): v for k, v in request_query.items()}
            start_ticks = int(q_lower.get("starttimeticks") or 0)
            start_seconds = start_ticks / 10_000_000
            log.info("HLS session %s start_ticks=%d (%.1fs)", session_id, start_ticks, start_seconds)

            # Probe duration and write static playlists once, before the NVENC→CPU retry loop.
            if session_dir.exists():
                shutil.rmtree(session_dir, ignore_errors=True)
            session_dir.mkdir(parents=True, exist_ok=True)
            for i in range(len(self.renditions)):
                (session_dir / f"v{i}").mkdir(parents=True, exist_ok=True)

            try:
                duration = await self._probe_duration(source_file)
            except Exception as exc:
                log.warning("ffprobe failed for %s (%s); defaulting to 7200s", source_file, exc)
                duration = 7200.0

            # The session covers only the portion of the video from start_seconds onwards.
            effective_duration = max(0.0, duration - start_seconds)
            total_segments = math.ceil(effective_duration / SEG_DURATION)

            tried_nvidia = False
            modes: list[bool] = [self.use_nvidia]
            if self.use_nvidia:
                modes.append(False)

            last_err: HTTPException | None = None
            for use_nvidia in modes:
                tried_nvidia = tried_nvidia or use_nvidia

                # On retry the previous ffmpeg run deleted the session dir via _stop_session;
                # recreate it and re-write the playlists.
                if last_err is not None:
                    session_dir.mkdir(parents=True, exist_ok=True)
                    for i in range(len(self.renditions)):
                        (session_dir / f"v{i}").mkdir(parents=True, exist_ok=True)

                # Pre-generate the complete static playlists so the player sees the full
                # timeline immediately and HLS.js can seek anywhere.  Actual segments are
                # produced on demand: wait_for_segment restarts ffmpeg if needed.
                self._write_static_playlists(session_dir, effective_duration, total_segments)

                ffmpeg_proc = await self._start_ffmpeg(
                    source_file=source_file,
                    out_dir=session_dir,
                    start_seconds=start_seconds,
                    start_segment=0,
                    use_nvidia=use_nvidia,
                )

                sess = HlsSession(
                    session_id=session_id,
                    item_id=item_id,
                    source_file=source_file,
                    dir_path=session_dir,
                    master_playlist_path=master_playlist_path,
                    started_at=time.time(),
                    last_access=time.time(),
                    ffmpeg_proc=ffmpeg_proc,
                    duration=effective_duration,
                    total_segments=total_segments,
                    start_seconds=start_seconds,
                    use_nvidia=use_nvidia,
                    ffmpeg_start_seg=0,
                )
                self._sessions[session_key] = sess

                # Brief liveness check: wait up to 1.5 s to catch an immediately-crashing
                # ffmpeg (e.g. unsupported NVENC codec) without blocking the HTTP response
                # for the full segment-production wait.  Actual segment delivery is handled
                # by wait_for_segment.
                try:
                    await self._check_ffmpeg_alive(ffmpeg_proc, timeout_s=2.0)
                    if tried_nvidia and not use_nvidia:
                        log.warning("Local HLS session %s fell back to CPU transcoding", session_id)
                    return sess
                except HTTPException as e:
                    last_err = e
                    await self._stop_session(session_key)
                    if use_nvidia:
                        log.warning(
                            "NVENC local HLS failed for %s, retrying with CPU: %s",
                            session_id,
                            e.detail,
                        )
                        continue
                    raise

            if last_err:
                raise last_err
            raise HTTPException(status_code=502, detail="Failed to start local HLS session")

    async def get_rewritten_master_playlist(self, session_id: str, base_url: str) -> str:
        sess = self._find_session_by_id(session_id)
        if not sess:
            raise HTTPException(status_code=404, detail="Unknown HLS session")

        sess.last_access = time.time()
        if not sess.master_playlist_path.exists():
            raise HTTPException(status_code=404, detail="Master playlist not ready")

        raw = sess.master_playlist_path.read_text()
        base = base_url.rstrip("/")
        out_lines: list[str] = []

        for line in raw.splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                out_lines.append(line)
                continue

            m = re.match(r"^v(\d+)/stream\.m3u8$", s)
            if m:
                variant = int(m.group(1))
                out_lines.append(
                    f"{base}/proxy/hls/session/{session_id}/variant/{variant}/playlist.m3u8"
                )
            else:
                out_lines.append(line)

        return "\n".join(out_lines) + "\n"

    async def get_rewritten_variant_playlist(
        self,
        session_id: str,
        variant: int,
        base_url: str,
    ) -> str:
        sess = self._find_session_by_id(session_id)
        if not sess:
            raise HTTPException(status_code=404, detail="Unknown HLS session")

        if variant < 0 or variant >= len(self.renditions):
            raise HTTPException(status_code=404, detail="Unknown HLS variant")

        playlist_path = self._variant_playlist_path(sess, variant)
        sess.last_access = time.time()

        if not playlist_path.exists() or playlist_path.stat().st_size == 0:
            raise HTTPException(status_code=404, detail="Variant playlist not ready")

        raw = playlist_path.read_text()
        base = base_url.rstrip("/")
        out_lines: list[str] = []

        for line in raw.splitlines():
            s = line.strip()
            if not s:
                out_lines.append(line)
                continue
            if s.startswith("#EXT-X-MAP:"):
                rewritten = re.sub(
                    r'URI="([^"]+)"',
                    lambda m: f'URI="{base}/proxy/hls/session/{session_id}/variant/{variant}/segment/{m.group(1)}"',
                    s,
                )
                out_lines.append(rewritten)
            elif s.startswith("#"):
                out_lines.append(line)
            else:
                out_lines.append(
                    f"{base}/proxy/hls/session/{session_id}/variant/{variant}/segment/{s}"
                )

        return "\n".join(out_lines) + "\n"

    async def wait_for_segment(
        self,
        session_id: str,
        variant: int,
        name: str,
        timeout_s: float = 30.0,
    ) -> Path:
        seg_path = self._variant_segment_path(session_id=session_id, variant=variant, name=name)
        sess = self._find_session_by_id(session_id)
        if not sess:
            raise HTTPException(status_code=404, detail="Unknown HLS session")

        if seg_path.exists() and seg_path.stat().st_size > 0:
            return seg_path

        # If the requested segment is far ahead of the current transcoding position, restart
        # ffmpeg at that segment instead of waiting for sequential transcoding to catch up.
        # Don't restart when latest < 0: ffmpeg hasn't produced anything yet and is still
        # starting up — requesting seg-000000 during normal startup must not kill it.
        m = re.match(r"^seg-(\d+)\.m4s$", name)
        if m:
            requested_seg = int(m.group(1))
            latest = self._latest_seg_num(sess, variant)
            needs_restart = (
                # Seeking to a non-zero segment before ffmpeg has produced anything
                (latest < 0 and requested_seg > 0)
                # Seeking far ahead of the current transcoding head
                or (latest >= 0 and requested_seg > latest + SEEK_RESTART_THRESHOLD)
            )
            if needs_restart:
                log.info(
                    "Session %s: segment %d requested, latest produced is %d — restarting ffmpeg at %d",
                    session_id, requested_seg, latest, requested_seg,
                )
                await self._restart_ffmpeg_at(sess, requested_seg)

        log.debug("Session %s: waiting for segment %s (variant %d)", session_id, name, variant)
        start = time.time()
        while time.time() - start < timeout_s:
            if seg_path.exists() and seg_path.stat().st_size > 0:
                log.debug(
                    "Session %s: segment %s ready after %.2fs",
                    session_id, name, time.time() - start,
                )
                return seg_path
            if not self._is_process_alive(sess.ffmpeg_proc):
                stderr = await self._read_proc_stderr(sess.ffmpeg_proc)
                raise HTTPException(
                    status_code=502,
                    detail=f"Local transcoder exited before segment ready: {stderr[-300:]}",
                )
            await asyncio.sleep(0.08)

        log.warning(
            "Session %s: timed out waiting for segment %s (variant %d) after %.1fs",
            session_id, name, variant, timeout_s,
        )
        raise HTTPException(status_code=404, detail="Segment not ready")

    def snapshot(self) -> dict:
        now = time.time()
        sessions = []
        for sess in self._sessions.values():
            proc = sess.ffmpeg_proc
            sessions.append(
                {
                    "session_id": sess.session_id,
                    "item_id": sess.item_id,
                    "source_file": str(sess.source_file),
                    "started_seconds_ago": round(now - sess.started_at, 1),
                    "last_access_seconds_ago": round(now - sess.last_access, 1),
                    "pid": proc.pid if proc else None,
                    "alive": self._is_process_alive(proc),
                }
            )

        return {
            "enabled": True,
            "configured_use_nvidia": self.use_nvidia,
            "ttl_seconds": self.ttl_seconds,
            "renditions": [
                {
                    "name": r.name,
                    "width": r.width,
                    "height": r.height,
                    "video_bitrate_k": r.video_bitrate_k,
                    "audio_bitrate_k": r.audio_bitrate_k,
                }
                for r in self.renditions
            ],
            "session_count": len(sessions),
            "sessions": sessions,
        }

    async def stop_all(self) -> None:
        keys = list(self._sessions.keys())
        for key in keys:
            await self._stop_session(key)

    # ── internals ────────────────────────────────────────────────────────────

    async def _cleanup_stale_sessions(self) -> None:
        now = time.time()
        stale_keys = [
            key
            for key, sess in self._sessions.items()
            if (now - sess.last_access) > self.ttl_seconds or not self._is_process_alive(sess.ffmpeg_proc)
        ]
        for key in stale_keys:
            await self._stop_session(key)

    async def _stop_session(self, key: str) -> None:
        sess = self._sessions.pop(key, None)
        if not sess:
            return

        proc = sess.ffmpeg_proc
        try:
            if self._is_process_alive(proc):
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=2)
                except TimeoutError:
                    proc.kill()
                    await proc.wait()
        except Exception:
            pass

        shutil.rmtree(sess.dir_path, ignore_errors=True)

    async def _probe_duration(self, source_file: Path) -> float:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(source_file),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15.0)
        return float(stdout.strip())

    def _write_static_playlists(
        self,
        session_dir: Path,
        duration: float,
        total_segments: int,
    ) -> None:
        # Master playlist — written by us so master.m3u8 is available before ffmpeg starts.
        lines = ["#EXTM3U", "#EXT-X-VERSION:7"]
        for i, r in enumerate(self.renditions):
            bandwidth = (r.video_bitrate_k + r.audio_bitrate_k) * 1000
            lines.append(
                f'#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},'
                f'RESOLUTION={r.width}x{r.height},'
                f'CODECS="avc1.640028,mp4a.40.2"'
            )
            lines.append(f"v{i}/stream.m3u8")
        (session_dir / "master.m3u8").write_text("\n".join(lines) + "\n")

        # Per-variant playlists — list every segment so HLS.js can seek anywhere.
        # Segments that haven't been transcoded yet will block in wait_for_segment until
        # ffmpeg produces them (or ffmpeg is restarted at the seek point).
        target_dur = int(SEG_DURATION) + 1
        for i in range(len(self.renditions)):
            variant_dir = session_dir / f"v{i}"
            pls = [
                "#EXTM3U",
                "#EXT-X-VERSION:7",
                f"#EXT-X-TARGETDURATION:{target_dur}",
                "#EXT-X-MEDIA-SEQUENCE:0",
                f'#EXT-X-MAP:URI="init_{i}.mp4"',
                "#EXT-X-INDEPENDENT-SEGMENTS",
            ]
            for seg_i in range(total_segments):
                if seg_i == total_segments - 1:
                    seg_len = duration - seg_i * SEG_DURATION
                else:
                    seg_len = SEG_DURATION
                pls.append(f"#EXTINF:{seg_len:.6f},")
                pls.append(f"seg-{seg_i:06d}.m4s")
            pls.append("#EXT-X-ENDLIST")
            (variant_dir / "stream.m3u8").write_text("\n".join(pls) + "\n")

    async def _check_ffmpeg_alive(
        self,
        proc: asyncio.subprocess.Process,
        timeout_s: float = 1.5,
    ) -> None:
        """Poll for up to timeout_s to catch an immediately-exiting ffmpeg process.

        If ffmpeg is still alive when timeout_s elapses we assume it's healthy and
        return without error — actual segment readiness is handled by wait_for_segment.
        """
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if not self._is_process_alive(proc):
                stderr = await self._read_proc_stderr(proc)
                raise HTTPException(
                    status_code=502,
                    detail=f"Local transcoder exited early: {stderr[-300:]}",
                )
            await asyncio.sleep(0.1)
        # Still alive — healthy.

    def _latest_seg_num(self, sess: HlsSession, variant: int) -> int:
        """Return the highest segment number produced so far, or -1 if none."""
        v_dir = sess.dir_path / f"v{variant}"
        max_seg = -1
        try:
            for f in v_dir.glob("seg-*.m4s"):
                m = re.match(r"^seg-(\d+)\.m4s$", f.name)
                if m:
                    max_seg = max(max_seg, int(m.group(1)))
        except Exception:
            pass
        return max_seg

    async def _restart_ffmpeg_at(self, sess: HlsSession, start_seg: int) -> None:
        proc = sess.ffmpeg_proc
        if self._is_process_alive(proc):
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=2)
            except TimeoutError:
                proc.kill()
                await proc.wait()

        start_seconds = sess.start_seconds + start_seg * SEG_DURATION
        new_proc = await self._start_ffmpeg(
            source_file=sess.source_file,
            out_dir=sess.dir_path,
            start_seconds=start_seconds,
            start_segment=start_seg,
            use_nvidia=sess.use_nvidia,
        )
        sess.ffmpeg_proc = new_proc
        sess.ffmpeg_start_seg = start_seg

    async def _start_ffmpeg(
        self,
        source_file: Path,
        out_dir: Path,
        start_seconds: float,
        start_segment: int,
        use_nvidia: bool,
    ) -> asyncio.subprocess.Process:
        # Write ffmpeg's own live playlist to a different name so it never overwrites
        # the pre-generated static stream.m3u8 that the proxy serves.
        seg_pattern = out_dir / "v%v" / "seg-%06d.m4s"
        output_pattern = out_dir / "v%v" / "stream_live.m3u8"

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
        ]

        if use_nvidia:
            cmd.extend(["-hwaccel", "cuda"])

        if start_seconds > 0:
            cmd.extend(["-ss", str(start_seconds)])

        cmd.extend(["-i", str(source_file)])

        for i, r in enumerate(self.renditions):
            cmd.extend(["-map", "0:v:0"])
            cmd.extend(["-map", "0:a:0?"])

            cmd.extend(
                [
                    f"-filter:v:{i}",
                    (
                        f"scale=w={r.width}:h={r.height}:"
                        "force_original_aspect_ratio=decrease:force_divisible_by=2"
                    ),
                ]
            )

            if use_nvidia:
                cmd.extend(
                    [
                        f"-c:v:{i}", "h264_nvenc",
                        f"-preset:v:{i}", "p4",
                        f"-rc:v:{i}", "vbr",
                        f"-cq:v:{i}", "23",
                    ]
                )
            else:
                cmd.extend(
                    [
                        f"-c:v:{i}", "libx264",
                        f"-preset:v:{i}", "veryfast",
                        f"-crf:v:{i}", "23",
                    ]
                )

            cmd.extend(
                [
                    f"-b:v:{i}", f"{r.video_bitrate_k}k",
                    f"-maxrate:v:{i}", f"{r.maxrate_k}k",
                    f"-bufsize:v:{i}", f"{r.bufsize_k}k",
                    f"-c:a:{i}", "aac",
                    f"-b:a:{i}", f"{r.audio_bitrate_k}k",
                ]
            )

        cmd.extend(
            [
                "-g", "48",
                "-keyint_min", "48",
                "-sc_threshold", "0",
                "-force_key_frames", "expr:gte(t,n_forced*2)",
                "-f", "hls",
                "-hls_time", str(int(SEG_DURATION)),
                "-hls_list_size", "0",
                "-hls_flags", "independent_segments",
                "-hls_segment_type", "fmp4",
                "-hls_fmp4_init_filename", "init_%v.mp4",
                "-start_number", str(start_segment),
                "-master_pl_name", "master_live.m3u8",
                "-hls_segment_filename", str(seg_pattern),
                "-var_stream_map", self._var_stream_map(),
                str(output_pattern),
            ]
        )

        log.info(
            "Starting local HLS transcoder (%s) from seg %d (%.1fs): %s",
            "nvidia" if use_nvidia else "cpu",
            start_segment,
            start_seconds,
            " ".join(cmd),
        )
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        self._stderr_buffers[proc.pid] = bytearray()
        asyncio.ensure_future(self._drain_stderr(proc))
        return proc

    async def _drain_stderr(self, proc: asyncio.subprocess.Process) -> None:
        if not proc.stderr:
            return
        try:
            data = await proc.stderr.read()
            if proc.pid in self._stderr_buffers:
                self._stderr_buffers[proc.pid].extend(data)
        except Exception:
            pass

    async def _read_proc_stderr(self, proc: asyncio.subprocess.Process) -> str:
        buf = self._stderr_buffers.pop(proc.pid, None)
        if buf:
            return bytes(buf).decode(errors="replace")
        if not proc.stderr:
            return ""
        try:
            data = await proc.stderr.read()
            return data.decode(errors="replace")
        except Exception:
            return ""

    def _find_session_by_id(self, session_id: str) -> HlsSession | None:
        for sess in self._sessions.values():
            if sess.session_id == session_id:
                return sess
        return None

    def _variant_playlist_path(self, sess: HlsSession, variant: int) -> Path:
        return sess.dir_path / f"v{variant}" / "stream.m3u8"

    def _variant_segment_path(self, session_id: str, variant: int, name: str) -> Path:
        sess = self._find_session_by_id(session_id)
        if not sess:
            raise HTTPException(status_code=404, detail="Unknown HLS session")
        if variant < 0 or variant >= len(self.renditions):
            raise HTTPException(status_code=404, detail="Unknown HLS variant")
        if "/" in name or "\\" in name or ".." in name:
            raise HTTPException(status_code=400, detail="Invalid segment name")

        sess.last_access = time.time()
        p = (sess.dir_path / f"v{variant}" / name).resolve()
        if not str(p).startswith(str((sess.dir_path / f"v{variant}").resolve()) + os.sep):
            raise HTTPException(status_code=400, detail="Invalid segment path")
        return p

    def _session_key(self, item_id: str, source_file: Path, request_query: dict) -> str:
        q = {k.lower(): v for k, v in request_query.items()}
        keep_keys = {
            "starttimeticks",
            "videobitrate",
            "audiobitrate",
            "maxwidth",
            "maxheight",
            "videocodec",
            "audiocodec",
        }
        normalized = {k: str(q.get(k, "")) for k in sorted(keep_keys)}
        serialized = f"{item_id}|{source_file}|{urlencode(normalized)}"
        return serialized

    def _session_id(self, session_key: str) -> str:
        return hashlib.sha1(session_key.encode()).hexdigest()[:16]

    def _var_stream_map(self) -> str:
        return " ".join(
            f"v:{i},a:{i}"
            for i in range(len(self.renditions))
        )

    @staticmethod
    def _is_process_alive(proc: asyncio.subprocess.Process) -> bool:
        return proc.returncode is None

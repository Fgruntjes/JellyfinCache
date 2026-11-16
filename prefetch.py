"""
Prefetch Daemon
===============
Periodically queries the remote Jellyfin for items the user is likely
to watch next, and kicks off downloads proactively.

Each poll cycle does two things:
  1. Eviction check — for every cached item, fetch its UserData from Jellyfin.
     If Played == true and LastPlayedDate + evict_after_watched_hours is in the
     past, evict the file.  This is Layer 2 of the eviction strategy: it covers
     restarts (which lose in-flight asyncio tasks from Layer 1) and items marked
     as watched on a different client that didn't go through this proxy.
  2. Prefetch — queue downloads for Continue Watching, Next Up and Favourites.

Run this as a separate process, or start it automatically via main.py.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx

from config import Config
from downloader import DownloadManager

log = logging.getLogger("prefetch")


class PrefetchDaemon:
    def __init__(self, cfg: Config, download_manager: DownloadManager):
        self.cfg = cfg
        self.dm = download_manager

    async def run(self, interval_seconds: int = 3600):
        """Run prefetch loop forever, polling every interval_seconds."""
        log.info("Prefetch daemon started (interval: %ds)", interval_seconds)
        while True:
            try:
                await self.prefetch_once()
            except Exception as e:
                log.error("Prefetch error: %s", e)
            await asyncio.sleep(interval_seconds)

    async def prefetch_once(self):
        log.info("Running prefetch pass")
        await self._check_evictions()
        item_ids = await self._get_candidates()
        log.info("Prefetch candidates: %d items", len(item_ids))
        for item_id in item_ids:
            asyncio.create_task(self.dm.enqueue(item_id))

    async def _get_users(self, client: httpx.AsyncClient) -> list[dict]:
        """Return Jellyfin users to prefetch for, filtered by JELLYFIN_PREFETCH_USERS if set."""
        resp = await client.get(
            f"{self.cfg.remote_url}/Users",
            headers=self.cfg.remote_auth_headers,
        )
        if resp.status_code != 200:
            log.warning("Could not fetch user list: HTTP %s", resp.status_code)
            return []
        all_users: list[dict] = resp.json()
        if not self.cfg.prefetch_users:
            return all_users
        allowed = {u.lower() for u in self.cfg.prefetch_users}
        filtered = [
            u for u in all_users
            if u["Id"].lower() in allowed or u.get("Name", "").lower() in allowed
        ]
        log.info(
            "Prefetch user filter active: %d/%d users selected",
            len(filtered), len(all_users),
        )
        return filtered

    async def _check_evictions(self):
        """Layer 2: poll-based eviction fallback.

        Walks every cached item, checks UserData.Played + LastPlayedDate via
        the Jellyfin API for every configured user, and evicts files whose
        eviction window has passed since the LAST time any user watched them.
        Handles server restarts and items watched outside this proxy.
        """
        if self.cfg.evict_after_watched_hours <= 0:
            return
        if not self.cfg.cache_dir.exists():
            return

        now = datetime.now(timezone.utc)
        headers = self.cfg.remote_auth_headers

        async with httpx.AsyncClient(timeout=15) as client:
            users = await self._get_users(client)
            if not users:
                return

            for item_dir in list(self.cfg.cache_dir.iterdir()):
                if not item_dir.is_dir() or not (item_dir / ".ready").exists():
                    continue
                item_id = item_dir.name

                # Collect the latest LastPlayedDate across all users
                latest_played: datetime | None = None

                for user in users:
                    try:
                        resp = await client.get(
                            f"{self.cfg.remote_url}/Users/{user['Id']}/Items/{item_id}",
                            headers=headers,
                        )
                        if resp.status_code != 200:
                            continue
                        ud = resp.json().get("UserData", {})
                    except Exception as e:
                        log.debug("Could not fetch UserData for %s/%s: %s",
                                  user.get("Name", user["Id"]), item_id, e)
                        continue

                    if not ud.get("Played"):
                        continue
                    lp_str = ud.get("LastPlayedDate")
                    if not lp_str:
                        continue
                    try:
                        lp = datetime.fromisoformat(lp_str.replace("Z", "+00:00"))
                        if latest_played is None or lp > latest_played:
                            latest_played = lp
                    except ValueError:
                        continue

                if latest_played is None:
                    continue

                evict_at = latest_played + timedelta(hours=self.cfg.evict_after_watched_hours)
                if now >= evict_at:
                    log.info(
                        "POLL EVICT [%s] — last watched %s, window of %.1fh passed",
                        item_id, latest_played.isoformat(), self.cfg.evict_after_watched_hours,
                    )
                    await self.dm.evict_item(item_id)

    async def _get_candidates(self) -> list[str]:
        """Return item IDs worth pre-downloading for all configured users."""
        headers = self.cfg.remote_auth_headers
        base = self.cfg.remote_url
        candidates = []

        async with httpx.AsyncClient(timeout=15) as client:
            users = await self._get_users(client)
            if not users:
                return []

            for user in users:
                user_id = user["Id"]
                user_name = user.get("Name", user_id)

                # --- Continue Watching ---
                resume_resp = await client.get(
                    f"{base}/Users/{user_id}/Items/Resume",
                    headers=headers,
                    params={"Limit": 10, "MediaTypes": "Video"},
                )
                if resume_resp.status_code == 200:
                    for item in resume_resp.json().get("Items", []):
                        candidates.append(item["Id"])
                        log.debug("[%s] Resume: %s", user_name, item.get("Name"))

                # --- Next episode for in-progress series ---
                next_up_resp = await client.get(
                    f"{base}/Shows/NextUp",
                    headers=headers,
                    params={"UserId": user_id, "Limit": 10},
                )
                if next_up_resp.status_code == 200:
                    for item in next_up_resp.json().get("Items", []):
                        candidates.append(item["Id"])
                        log.debug("[%s] Next up: %s", user_name, item.get("Name"))

                candidates.extend(
                    await self._get_favourite_candidates(client, user_id, user_name)
                )

        # Deduplicate while preserving order
        seen: set[str] = set()
        result: list[str] = []
        for item_id in candidates:
            if item_id not in seen:
                seen.add(item_id)
                result.append(item_id)

        return result

    async def _get_favourite_candidates(
        self, client: httpx.AsyncClient, user_id: str, user_name: str
    ) -> list[str]:
        headers = self.cfg.remote_auth_headers
        base = self.cfg.remote_url
        favourites_resp = await client.get(
            f"{base}/Items",
            headers=headers,
            params={
                "userId": user_id,
                "Filters": "IsFavorite",
                "Recursive": "true",
                "IncludeItemTypes": "Movie,Episode,Series",
            },
        )
        if favourites_resp.status_code != 200:
            return []

        candidates = []
        for item in favourites_resp.json().get("Items", []):
            if item.get("Type") != "Series":
                candidates.append(item["Id"])
                log.debug("[%s] Favourite: %s", user_name, item.get("Name"))
                continue
            episodes_resp = await client.get(
                f"{base}/Items",
                headers=headers,
                params={
                    "userId": user_id,
                    "ParentId": item["Id"],
                    "Recursive": "true",
                    "IncludeItemTypes": "Episode",
                    "Filters": "IsUnplayed",
                    "IsMissing": "false",
                    "SortBy": "ParentIndexNumber,IndexNumber",
                    "Limit": self.cfg.prefetch_favourite_episodes,
                },
            )
            if episodes_resp.status_code == 200:
                for episode in episodes_resp.json().get("Items", []):
                    candidates.append(episode["Id"])
                    log.debug("[%s] Favourite series %s: %s",
                              user_name, item.get("Name"), episode.get("Name"))
        return candidates


async def main():
    """Run the prefetch daemon standalone."""
    import os
    from config import Config
    from downloader import DownloadManager

    cfg = Config.load()
    dm = DownloadManager(cfg)
    daemon = PrefetchDaemon(cfg, dm)

    interval = int(os.getenv("JELLYFIN_PREFETCH_INTERVAL_SECONDS", "3600"))
    await daemon.run(interval_seconds=interval)


if __name__ == "__main__":
    asyncio.run(main())

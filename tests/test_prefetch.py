import asyncio
from pathlib import Path

import httpx
import pytest

import prefetch
from config import Config
from prefetch import PrefetchDaemon

REMOTE = "https://jellyfin.test"


def jellyfin_with_favourites(favourites, unplayed_episodes_by_series):
    def respond(request: httpx.Request) -> httpx.Response:
        path, params = request.url.path, request.url.params
        if path == "/Users":
            return httpx.Response(200, json=[{"Id": "friend", "Name": "Friend"}])
        if path == "/Items" and params.get("Filters") == "IsFavorite":
            return httpx.Response(200, json={"Items": favourites})
        if path == "/Items" and params.get("Filters") == "IsUnplayed":
            episodes = unplayed_episodes_by_series[params["ParentId"]]
            return httpx.Response(200, json={"Items": episodes[: int(params["Limit"])]})
        return httpx.Response(200, json={"Items": []})

    return httpx.MockTransport(respond)


@pytest.fixture
def daemon_for(monkeypatch):
    def use_transport(transport):
        real_client = httpx.AsyncClient
        monkeypatch.setattr(
            prefetch.httpx,
            "AsyncClient",
            lambda **kwargs: real_client(transport=transport, **kwargs),
        )
        cfg = Config(
            remote_url=REMOTE,
            remote_api_key="key",
            remote_user_id="friend",
            cache_dir=Path("/nonexistent"),
            prefetch_favourite_episodes=2,
        )
        return PrefetchDaemon(cfg, download_manager=None)

    return use_transport


def test_favourite_movies_and_episodes_are_prefetch_candidates(daemon_for):
    transport = jellyfin_with_favourites(
        favourites=[{"Id": "movie", "Type": "Movie"}, {"Id": "episode", "Type": "Episode"}],
        unplayed_episodes_by_series={},
    )

    candidates = asyncio.run(daemon_for(transport)._get_candidates())

    assert candidates == ["movie", "episode"]


def test_favourite_series_expands_to_next_unplayed_episodes(daemon_for):
    transport = jellyfin_with_favourites(
        favourites=[{"Id": "series", "Type": "Series"}],
        unplayed_episodes_by_series={
            "series": [{"Id": "s01e03"}, {"Id": "s01e04"}, {"Id": "s01e05"}]
        },
    )

    candidates = asyncio.run(daemon_for(transport)._get_candidates())

    assert candidates == ["s01e03", "s01e04"]

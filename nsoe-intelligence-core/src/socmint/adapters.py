"""
Social media ingestion adapters — 2.2.1
  - Twitter/X filtered stream API
  - Telegram public channel scraper (polling via Bot API)
  - News RSS aggregator (50+ Indian news sources)
All adapters produce raw dicts onto the socmint.raw.events Kafka topic.
"""

from __future__ import annotations

import feedparser
import hashlib
import json
import os
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Iterator, List, Optional

import requests
import structlog

logger = structlog.get_logger(__name__)

# ── Base ─────────────────────────────────────────────────────

class SocmintAdapter(ABC):
    platform: str

    @abstractmethod
    def stream(self) -> Iterator[dict]: ...

    def _make_raw(self, post_id: str, text: str, author_id: Optional[str],
                  author_handle: Optional[str], url: Optional[str],
                  posted_at_ms: int) -> dict:
        return {
            "event_id":       str(uuid.uuid4()),
            "platform":       self.platform,
            "post_id":        post_id,
            "author_id":      author_id,
            "author_handle":  author_handle,
            "text":           text,
            "url":            url,
            "posted_at_ms":   posted_at_ms,
            "received_at_ms": int(time.time() * 1000),
        }


# ── Twitter/X ────────────────────────────────────────────────

class TwitterAdapter(SocmintAdapter):
    """
    Uses the Twitter v2 filtered stream API.
    Requires a Bearer Token with Elevated access.
    Rules are configured via the /2/tweets/search/stream/rules endpoint.
    """
    platform = "TWITTER"

    _STREAM_URL = "https://api.twitter.com/2/tweets/search/stream"
    _RULES_URL  = "https://api.twitter.com/2/tweets/search/stream/rules"
    _PARAMS = {
        "tweet.fields": "created_at,author_id,entities,lang,public_metrics",
        "expansions":   "author_id",
        "user.fields":  "username,verified",
    }

    def __init__(self, bearer_token: str, keywords: List[str], user_watchlist: List[str]) -> None:
        self._token = bearer_token
        self._keywords = keywords
        self._user_watchlist = user_watchlist
        self._headers = {"Authorization": f"Bearer {self._token}"}

    def _sync_rules(self) -> None:
        """Upsert stream filter rules."""
        resp = requests.get(self._RULES_URL, headers=self._headers, timeout=10)
        existing = {r["value"] for r in resp.json().get("data", [])}
        rules = [{"value": kw} for kw in self._keywords if kw not in existing]
        rules += [{"value": f"from:{u}"} for u in self._user_watchlist
                  if f"from:{u}" not in existing]
        if rules:
            requests.post(self._RULES_URL, headers=self._headers,
                          json={"add": rules}, timeout=10)
            logger.info("twitter_rules_synced", added=len(rules))

    def stream(self) -> Iterator[dict]:
        self._sync_rules()
        while True:
            try:
                with requests.get(
                    self._STREAM_URL, params=self._PARAMS,
                    headers=self._headers, stream=True, timeout=30,
                ) as resp:
                    resp.raise_for_status()
                    for line in resp.iter_lines():
                        if not line:
                            continue
                        data = json.loads(line)
                        tweet = data.get("data", {})
                        if not tweet:
                            continue
                        posted_ms = int(
                            datetime.fromisoformat(
                                tweet.get("created_at", "").replace("Z", "+00:00")
                            ).timestamp() * 1000
                        ) if tweet.get("created_at") else int(time.time() * 1000)
                        yield self._make_raw(
                            post_id=tweet.get("id", ""),
                            text=tweet.get("text", ""),
                            author_id=tweet.get("author_id"),
                            author_handle=None,
                            url=f"https://twitter.com/i/web/status/{tweet.get('id','')}",
                            posted_at_ms=posted_ms,
                        )
            except Exception as e:
                logger.warning("twitter_stream_error", error=str(e))
                time.sleep(5)


# ── Telegram ────────────────────────────────────────────────

class TelegramAdapter(SocmintAdapter):
    """
    Polls Telegram public channels via the Bot API.
    Requires bot token with access to target channels.
    Lawful authorization required before deployment.
    """
    platform = "TELEGRAM"
    _API_BASE = "https://api.telegram.org/bot{token}/getUpdates"

    def __init__(self, bot_token: str, channel_ids: List[str]) -> None:
        self._token = bot_token
        self._channel_ids = channel_ids
        self._offset: dict = {}  # channel_id → last_update_id

    def stream(self) -> Iterator[dict]:
        while True:
            for channel_id in self._channel_ids:
                try:
                    yield from self._poll_channel(channel_id)
                except Exception as e:
                    logger.warning("telegram_poll_error", channel=channel_id, error=str(e))
            time.sleep(30)

    def _poll_channel(self, channel_id: str) -> Iterator[dict]:
        url = self._API_BASE.format(token=self._token)
        params = {"chat_id": channel_id, "limit": 100,
                  "offset": self._offset.get(channel_id, 0)}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        for update in resp.json().get("result", []):
            msg = update.get("message") or update.get("channel_post", {})
            if not msg:
                continue
            self._offset[channel_id] = update["update_id"] + 1
            text = msg.get("text") or msg.get("caption", "")
            if not text:
                continue
            posted_ms = msg.get("date", int(time.time())) * 1000
            sender = msg.get("from", {})
            yield self._make_raw(
                post_id=str(msg.get("message_id", "")),
                text=text,
                author_id=str(sender.get("id", "")),
                author_handle=sender.get("username"),
                url=None,
                posted_at_ms=posted_ms,
            )


# ── RSS / News aggregator ────────────────────────────────────

INDIAN_NEWS_FEEDS = [
    "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
    "https://feeds.feedburner.com/ndtvnews-top-stories",
    "https://www.thehindu.com/news/feeder/default.rss",
    "https://indianexpress.com/feed/",
    "https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
    "https://www.livemint.com/rss/news",
    "https://feeds.feedburner.com/zeenews/india",
    "https://www.deccanherald.com/rss-feeds/national.rss",
    "https://feeds.bbci.co.uk/hindi/rss.xml",
    "https://aajtak.intoday.in/rss/topstories.xml",
]


class RSSNewsAdapter(SocmintAdapter):
    platform = "RSS"

    def __init__(self, feed_urls: Optional[List[str]] = None,
                 poll_interval_s: int = 300) -> None:
        self._feeds = feed_urls or INDIAN_NEWS_FEEDS
        self._poll_interval_s = poll_interval_s
        self._seen: set = set()

    def stream(self) -> Iterator[dict]:
        while True:
            for url in self._feeds:
                try:
                    yield from self._poll_feed(url)
                except Exception as e:
                    logger.debug("rss_error", url=url, error=str(e))
            time.sleep(self._poll_interval_s)

    def _poll_feed(self, url: str) -> Iterator[dict]:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            link = entry.get("link", "")
            post_id = hashlib.md5(link.encode()).hexdigest()
            if post_id in self._seen:
                continue
            self._seen.add(post_id)
            # Keep seen set bounded
            if len(self._seen) > 50_000:
                self._seen = set(list(self._seen)[-25_000:])

            summary = entry.get("summary", "") or entry.get("description", "")
            title = entry.get("title", "")
            text = f"{title}. {summary}"

            published = entry.get("published_parsed")
            if published:
                posted_ms = int(time.mktime(published) * 1000)
            else:
                posted_ms = int(time.time() * 1000)

            yield self._make_raw(
                post_id=post_id,
                text=text[:2000],
                author_id=None,
                author_handle=feed.feed.get("title"),
                url=link,
                posted_at_ms=posted_ms,
            )

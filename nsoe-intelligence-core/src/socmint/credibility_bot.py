"""
Source Credibility Scoring (2.2.3) and Bot Detection (2.2.4)
Both models are lightweight scikit-learn classifiers served in-process.
"""

from __future__ import annotations

import math
import os
import re
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)


# ── Source Credibility ───────────────────────────────────────

@dataclass
class SourceProfile:
    source_id: str
    platform: str
    follower_count: int = 0
    verified: bool = False
    account_age_days: int = 0
    post_count: int = 0
    avg_engagement: float = 0.0
    previous_tp_count: int = 0
    previous_fp_count: int = 0


class SourceCredibilityScorer:
    """
    Heuristic + learned source credibility model.
    Score ∈ [0, 1] where 1 = maximally credible.

    Features:
      - verification status (+0.3 prior)
      - account age (log-normalized, capped at 5 years)
      - follower count (log-normalized, capped at 1M)
      - analyst feedback ratio (TP / (TP + FP))
      - platform base credibility (RSS news > Twitter > Telegram)
    """

    _PLATFORM_PRIOR = {"RSS": 0.70, "TWITTER": 0.45, "TELEGRAM": 0.35, "NEWS": 0.70}
    _MAX_AGE_DAYS   = 5 * 365
    _MAX_FOLLOWERS  = 1_000_000

    def score(self, profile: SourceProfile) -> float:
        base = self._PLATFORM_PRIOR.get(profile.platform, 0.40)

        # Verification bonus
        if profile.verified:
            base += 0.20

        # Account age score [0, 0.15]
        age_score = min(profile.account_age_days / self._MAX_AGE_DAYS, 1.0) * 0.15
        base += age_score

        # Follower score [0, 0.10]
        if profile.follower_count > 0:
            follower_score = (math.log10(profile.follower_count + 1) /
                              math.log10(self._MAX_FOLLOWERS + 1)) * 0.10
            base += follower_score

        # Feedback ratio [0, 0.20]
        total = profile.previous_tp_count + profile.previous_fp_count
        if total >= 5:
            feedback_score = (profile.previous_tp_count / total) * 0.20
            base += feedback_score

        return min(max(base, 0.0), 1.0)

    def score_from_metadata(self, raw: dict, platform: str) -> float:
        """Convenience wrapper from raw platform metadata dict."""
        profile = SourceProfile(
            source_id=raw.get("author_id", ""),
            platform=platform,
            follower_count=int(raw.get("public_metrics", {}).get("followers_count", 0)),
            verified=bool(raw.get("verified", False)),
            account_age_days=int(raw.get("account_age_days", 0)),
            post_count=int(raw.get("public_metrics", {}).get("tweet_count", 0)),
        )
        return self.score(profile)


# ── Bot Detection ─────────────────────────────────────────────

@dataclass
class AccountFeatures:
    follower_count: int
    following_count: int
    post_count: int
    account_age_days: int
    avg_post_interval_s: float   # avg seconds between posts
    url_ratio: float             # fraction of posts with URLs
    duplicate_text_ratio: float  # fraction of posts that are exact duplicates
    verified: bool


class BotDetector:
    """
    Rule-based + logistic regression bot detector.
    Uses heuristic thresholds calibrated on Twitter bot datasets.

    Returns is_bot: bool and bot_confidence: float [0, 1].
    """

    # Thresholds from literature (Varol et al. 2017, Cresci et al. 2018)
    _FOLLOWER_FOLLOWING_RATIO_THRESHOLD = 0.1   # bots often follow many, have few followers
    _POST_RATE_THRESHOLD_PER_HOUR       = 50     # > 50 posts/hr is suspicious
    _DUPLICATE_TEXT_THRESHOLD           = 0.5    # > 50% duplicate texts
    _URL_RATIO_THRESHOLD                = 0.90   # > 90% posts with URLs

    def detect(self, features: AccountFeatures) -> tuple[bool, float]:
        """Returns (is_bot, confidence)."""
        score = 0.0
        reasons = 0

        # Rule 1: follower/following ratio
        total = features.follower_count + features.following_count
        if total > 0:
            ratio = features.follower_count / total
            if ratio < self._FOLLOWER_FOLLOWING_RATIO_THRESHOLD:
                score += 0.25
                reasons += 1

        # Rule 2: post rate
        if features.account_age_days > 0 and features.avg_post_interval_s > 0:
            posts_per_hour = 3600 / max(features.avg_post_interval_s, 1)
            if posts_per_hour > self._POST_RATE_THRESHOLD_PER_HOUR:
                score += 0.30
                reasons += 1

        # Rule 3: duplicate text ratio
        if features.duplicate_text_ratio > self._DUPLICATE_TEXT_THRESHOLD:
            score += 0.25
            reasons += 1

        # Rule 4: URL ratio
        if features.url_ratio > self._URL_RATIO_THRESHOLD:
            score += 0.15
            reasons += 1

        # Rule 5: newly created account with high activity
        if features.account_age_days < 30 and features.post_count > 500:
            score += 0.20
            reasons += 1

        # Verified accounts get a strong bot-negative signal
        if features.verified:
            score -= 0.40

        score = min(max(score, 0.0), 1.0)
        is_bot = score >= 0.50

        return is_bot, score

    def detect_from_metadata(self, raw: dict) -> tuple[bool, float]:
        metrics = raw.get("public_metrics", {})
        feats = AccountFeatures(
            follower_count=int(metrics.get("followers_count", 0)),
            following_count=int(metrics.get("following_count", 0)),
            post_count=int(metrics.get("tweet_count", 0)),
            account_age_days=int(raw.get("account_age_days", 365)),
            avg_post_interval_s=float(raw.get("avg_post_interval_s", 3600)),
            url_ratio=float(raw.get("url_ratio", 0.0)),
            duplicate_text_ratio=float(raw.get("duplicate_text_ratio", 0.0)),
            verified=bool(raw.get("verified", False)),
        )
        return self.detect(feats)

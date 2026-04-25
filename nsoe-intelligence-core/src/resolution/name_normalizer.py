"""
Indian Name Normalizer — 2.4.2 / 2.4.3
Handles transliterations (Mohammed/Muhammad/Mohammad),
alias clustering for known terrorist group names.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Dict, List, Optional, Set, Tuple

# ── Transliteration groups ────────────────────────────────────
# Each group is a canonical form → set of known variants
NAME_CLUSTERS: Dict[str, Set[str]] = {
    "MUHAMMAD": {
        "muhammad", "mohammed", "mohammad", "muhammed", "mohamad",
        "mehmet", "muhamed", "mahomed", "mahomet", "muhamad",
    },
    "ALI": {"ali", "alee", "aly"},
    "HUSSAIN": {"hussain", "husain", "hussein", "husayn", "hossain", "hossein"},
    "IBRAHIM": {"ibrahim", "ebrahim", "ibraahim", "ibraheem"},
    "ABDALLAH": {"abdallah", "abdullah", "abdulla", "abd allah"},
    "OSAMA": {"osama", "usama", "usamah"},
    "DAWOOD": {"dawood", "dawud", "daud", "daoud"},
}

# Reverse lookup: variant → canonical
_VARIANT_TO_CANONICAL: Dict[str, str] = {}
for canonical, variants in NAME_CLUSTERS.items():
    for v in variants:
        _VARIANT_TO_CANONICAL[v] = canonical

# ── Terrorist group alias clusters ───────────────────────────
GROUP_ALIASES: Dict[str, List[str]] = {
    "LASHKAR_E_TAIBA": [
        "lashkar-e-taiba", "lashkar-e-tayyiba", "let", "jud",
        "jamaat ud dawa", "jamat ud dawah", "lashkar e taiba",
        "lashkar e tayyaba", "the resistance front", "trf",
    ],
    "JAISH_E_MOHAMMED": [
        "jaish-e-mohammed", "jaish e mohammed", "jem", "jaish-e-mohammad",
        "khuddam ul islam", "tehrik ul furqaan",
    ],
    "INDIAN_MUJAHIDEEN": [
        "indian mujahideen", "im", "indian mujahideen movement",
    ],
    "HIZB_UL_MUJAHIDEEN": [
        "hizbul mujahideen", "hizb ul mujahideen", "hm",
    ],
    "SIMI": [
        "simi", "students islamic movement of india",
        "students islamic movement",
    ],
    "NAXAL": [
        "naxal", "maoist", "cpi maoist", "cpi-maoist", "pwg",
        "peoples war group", "naxalite", "red corridor",
    ],
    "ULFA": [
        "ulfa", "united liberation front of assam",
        "united liberation front of asom", "ulfa-i",
    ],
}

_GROUP_VARIANT_TO_CANONICAL: Dict[str, str] = {}
for canonical, aliases in GROUP_ALIASES.items():
    for alias in aliases:
        _GROUP_VARIANT_TO_CANONICAL[alias.lower()] = canonical


def normalize_person_name(name: str) -> str:
    """
    Return canonical form of an Indian personal name.
    Steps:
      1. Unicode normalize (NFC)
      2. Lowercase + strip punctuation
      3. Check transliteration cluster
      4. Return original capitalised if no cluster match
    """
    nfc = unicodedata.normalize("NFC", name.strip())
    clean = re.sub(r"[^a-z\s]", "", nfc.lower()).strip()
    tokens = clean.split()
    canonical_tokens = [_VARIANT_TO_CANONICAL.get(t, t) for t in tokens]
    result = " ".join(canonical_tokens).upper()
    return result


def normalize_group_name(name: str) -> str:
    """Return canonical group name from alias lookup."""
    key = re.sub(r"\s+", " ", name.strip().lower())
    return _GROUP_VARIANT_TO_CANONICAL.get(key, name.upper())


def name_similarity(a: str, b: str) -> float:
    """
    Jaro-Winkler similarity for name matching.
    Returns [0.0, 1.0].
    """
    na = normalize_person_name(a)
    nb = normalize_person_name(b)
    if na == nb:
        return 1.0
    return _jaro_winkler(na.lower(), nb.lower())


def _jaro(s1: str, s2: str) -> float:
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    match_distance = max(len1, len2) // 2 - 1
    match_distance = max(0, match_distance)
    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    transpositions = 0
    for i in range(len1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1
    return (matches / len1 + matches / len2 +
            (matches - transpositions / 2) / matches) / 3


def _jaro_winkler(s1: str, s2: str, p: float = 0.1) -> float:
    jaro = _jaro(s1, s2)
    prefix = 0
    for i in range(min(4, len(s1), len(s2))):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break
    return jaro + prefix * p * (1.0 - jaro)

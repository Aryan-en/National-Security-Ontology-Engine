"""
MITRE ATT&CK TTP Mapping Service — 2.1.3
Maps observed cyber technique signatures to ATT&CK technique IDs.
Uses the official MITRE ATT&CK STIX bundle (enterprise-attack.json).
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import structlog

logger = structlog.get_logger(__name__)

ATTACK_BUNDLE_PATH = os.environ.get(
    "MITRE_ATTACK_BUNDLE_PATH",
    "/etc/nsoe/mitre/enterprise-attack.json",
)


@dataclass(frozen=True)
class AttackTechnique:
    technique_id: str   # e.g. T1059.001
    name: str
    tactic: str         # e.g. execution
    description: str
    detection_hint: str


class MitreAttackMapper:
    """
    In-memory index of ATT&CK techniques.
    Provides:
      - lookup_by_id(technique_id) → AttackTechnique
      - map_event(event_text) → List[AttackTechnique] (keyword match)
      - map_stix_pattern(pattern) → List[str] technique IDs
    """

    def __init__(self) -> None:
        self._by_id: Dict[str, AttackTechnique] = {}
        self._keyword_index: Dict[str, List[str]] = {}  # keyword → [technique_id]

    def load(self, bundle_path: str = ATTACK_BUNDLE_PATH) -> None:
        p = Path(bundle_path)
        if not p.exists():
            logger.warning("attack_bundle_not_found", path=bundle_path)
            self._load_minimal_fallback()
            return

        with open(p) as f:
            bundle = json.load(f)

        tactics: Dict[str, str] = {}
        for obj in bundle.get("objects", []):
            if obj.get("type") == "x-mitre-tactic":
                tactics[obj["x_mitre_shortname"]] = obj["name"]

        for obj in bundle.get("objects", []):
            if obj.get("type") != "attack-pattern":
                continue
            tech_id = next(
                (r["external_id"] for r in obj.get("external_references", [])
                 if r.get("source_name") == "mitre-attack"),
                None,
            )
            if not tech_id:
                continue
            kill_chain = obj.get("kill_chain_phases", [])
            tactic = kill_chain[0]["phase_name"] if kill_chain else "unknown"
            desc = obj.get("description", "")
            detection = obj.get("x_mitre_detection", "")
            tech = AttackTechnique(
                technique_id=tech_id,
                name=obj.get("name", ""),
                tactic=tactic,
                description=desc[:300],
                detection_hint=detection[:300],
            )
            self._by_id[tech_id] = tech
            for kw in self._extract_keywords(obj.get("name", "") + " " + desc):
                self._keyword_index.setdefault(kw, []).append(tech_id)

        logger.info("attack_mapper_loaded", techniques=len(self._by_id))

    def _extract_keywords(self, text: str) -> List[str]:
        words = re.findall(r"\b[a-zA-Z]{4,}\b", text.lower())
        stopwords = {"with", "that", "this", "from", "have", "been", "they",
                     "used", "uses", "when", "which", "where", "also", "such",
                     "other", "their", "often", "using", "these", "attack"}
        return [w for w in set(words) if w not in stopwords]

    def lookup_by_id(self, technique_id: str) -> Optional[AttackTechnique]:
        return self._by_id.get(technique_id)

    def map_event(self, event_text: str, top_k: int = 5) -> List[AttackTechnique]:
        """Keyword-based technique mapping for a raw log line or description."""
        words = set(re.findall(r"\b[a-zA-Z]{4,}\b", event_text.lower()))
        scores: Dict[str, int] = {}
        for w in words:
            for tid in self._keyword_index.get(w, []):
                scores[tid] = scores.get(tid, 0) + 1
        top_ids = sorted(scores, key=scores.__getitem__, reverse=True)[:top_k]
        return [self._by_id[tid] for tid in top_ids if tid in self._by_id]

    def map_stix_pattern(self, pattern: str) -> List[str]:
        """Return technique IDs referenced in a STIX pattern or description."""
        matches = re.findall(r"T\d{4}(?:\.\d{3})?", pattern)
        return [m for m in matches if m in self._by_id]

    def _load_minimal_fallback(self) -> None:
        """Hard-coded subset for testing without the full MITRE bundle."""
        fallback = [
            ("T1059.001", "PowerShell", "execution", "Adversary uses PowerShell"),
            ("T1055",     "Process Injection", "defense-evasion", "Injecting code into processes"),
            ("T1071.001", "Web Protocols", "command-and-control", "C2 over HTTP/S"),
            ("T1486",     "Data Encrypted for Impact", "impact", "Ransomware encryption"),
            ("T1078",     "Valid Accounts", "defense-evasion", "Using legitimate credentials"),
            ("T1566.001", "Spearphishing Attachment", "initial-access", "Phishing via attachment"),
            ("T1190",     "Exploit Public-Facing Application", "initial-access", "Web app exploit"),
            ("T1021.001", "Remote Desktop Protocol", "lateral-movement", "RDP lateral movement"),
            ("T1003.001", "LSASS Memory", "credential-access", "Credential dumping from LSASS"),
            ("T1083",     "File and Directory Discovery", "discovery", "Enumerate filesystem"),
        ]
        for tid, name, tactic, desc in fallback:
            tech = AttackTechnique(technique_id=tid, name=name, tactic=tactic,
                                   description=desc, detection_hint="")
            self._by_id[tid] = tech
            for kw in self._extract_keywords(name + " " + desc):
                self._keyword_index.setdefault(kw, []).append(tid)
        logger.info("attack_mapper_fallback_loaded", techniques=len(self._by_id))


# Module-level singleton
_mapper: Optional[MitreAttackMapper] = None


def get_mapper() -> MitreAttackMapper:
    global _mapper
    if _mapper is None:
        _mapper = MitreAttackMapper()
        _mapper.load()
    return _mapper

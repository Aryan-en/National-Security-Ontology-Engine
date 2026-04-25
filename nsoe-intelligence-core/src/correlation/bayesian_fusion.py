"""
Bayesian Evidence Fusion — 2.3.3
Combines independent evidence signals using a probabilistic model.
Based on Dempster-Shafer theory adapted for multi-domain fusion.

Output: fused_confidence_score ∈ [0, 1] with contributing signals list.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class EvidenceSignal:
    """A single piece of evidence from one domain."""
    domain: str              # CCTV | CYBER | SOCMINT
    signal_type: str         # SIGHTING | ANOMALY | THREAT_POST | LOITERING | etc.
    confidence: float        # raw confidence ∈ [0, 1]
    source_credibility: float = 1.0   # source reliability ∈ [0, 1]
    event_id: str = ""


@dataclass
class FusionResult:
    fused_confidence: float
    alert_tier: str
    contributing_signals: List[EvidenceSignal]
    domain_contributions: Dict[str, float]
    explanation: str


class BayesianFusion:
    """
    Bayesian fusion using the Naïve Bayes assumption of signal independence,
    weighted by source credibility.

    Formula:
        P(threat | e1, e2, ...) = 1 - ∏(1 - w_i * P(e_i | threat))

    where w_i is the source credibility weight for signal i.
    """

    # Base prior — probability of a random entity being a threat
    PRIOR_THREAT = 0.02

    # Domain weights (can be calibrated from historical data)
    DOMAIN_WEIGHTS: Dict[str, float] = {
        "CCTV":    0.70,
        "CYBER":   0.85,
        "SOCMINT": 0.55,
    }

    def fuse(self, signals: List[EvidenceSignal]) -> FusionResult:
        if not signals:
            return FusionResult(
                fused_confidence=0.0,
                alert_tier="ANALYST_REVIEW",
                contributing_signals=[],
                domain_contributions={},
                explanation="No evidence signals",
            )

        # Group by domain to track independent contributions
        by_domain: Dict[str, List[EvidenceSignal]] = {}
        for sig in signals:
            by_domain.setdefault(sig.domain, []).append(sig)

        # Compute domain-level evidence probability
        # P(evidence | threat) for each domain using OR-gate combination
        domain_probs: Dict[str, float] = {}
        for domain, sigs in by_domain.items():
            dw = self.DOMAIN_WEIGHTS.get(domain, 0.5)
            # Within a domain: combine multiple signals with OR-gate
            p_no_evidence = 1.0
            for sig in sigs:
                effective_conf = sig.confidence * sig.source_credibility * dw
                p_no_evidence *= (1.0 - effective_conf)
            domain_probs[domain] = 1.0 - p_no_evidence

        # Cross-domain fusion: multiply domain evidence (independence assumption)
        # Bayesian update: P(threat | all_evidence) using log-odds
        prior_odds = self.PRIOR_THREAT / (1.0 - self.PRIOR_THREAT)
        log_odds = math.log(prior_odds)

        for domain, p_evidence_given_threat in domain_probs.items():
            # Likelihood ratio: P(evidence|threat) / P(evidence|no_threat)
            # P(evidence|no_threat) = base rate of false positives ≈ 0.05
            p_evidence_given_no_threat = max(0.05, 1.0 - p_evidence_given_threat)
            lr = p_evidence_given_threat / p_evidence_given_no_threat
            log_odds += math.log(max(lr, 1e-9))

        posterior_odds = math.exp(log_odds)
        fused_confidence = posterior_odds / (1.0 + posterior_odds)

        # Boost when multiple independent domains corroborate
        if len(by_domain) >= 3:
            fused_confidence = min(fused_confidence * 1.3, 1.0)
        elif len(by_domain) >= 2:
            fused_confidence = min(fused_confidence * 1.15, 1.0)

        tier = _tier(fused_confidence)
        explanation = (
            f"Fused {len(signals)} signals from {len(by_domain)} domain(s): "
            + ", ".join(f"{d}={v:.2f}" for d, v in domain_probs.items())
        )

        return FusionResult(
            fused_confidence=round(fused_confidence, 4),
            alert_tier=tier,
            contributing_signals=signals,
            domain_contributions=domain_probs,
            explanation=explanation,
        )

    def tier_score(self, confidence: float) -> str:
        return _tier(confidence)


def _tier(score: float) -> str:
    if score >= 0.80: return "ESCALATION"
    if score >= 0.60: return "SOFT_ALERT"
    if score >= 0.40: return "ANALYST_REVIEW"
    return "BELOW_THRESHOLD"

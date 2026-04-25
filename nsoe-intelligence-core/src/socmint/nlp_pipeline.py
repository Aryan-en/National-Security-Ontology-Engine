"""
NLP Pipeline — 2.2.2
  - Language detection (langdetect)
  - Multilingual NER (XLM-RoBERTa fine-tuned on Indian content)
  - Threat intent classification (IndicBERT fine-tuned)
  - Entity extraction: PER / LOC / ORG / WEAPON / EVENT
  - Cross-language entity normalization placeholder (mBART-50 translation)
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import structlog

logger = structlog.get_logger(__name__)

NER_MODEL_NAME    = os.environ.get("NER_MODEL_NAME",    "xlm-roberta-base")
INTENT_MODEL_NAME = os.environ.get("INTENT_MODEL_NAME", "ai4bharat/indic-bert")
TRANSLATE_MODEL   = os.environ.get("TRANSLATE_MODEL",   "facebook/mbart-large-50-many-to-many-mmt")
DEVICE            = os.environ.get("NLP_DEVICE",        "cpu")   # or "cuda"
MAX_LEN           = 512


@dataclass
class NEREntity:
    text: str
    label: str           # PER | LOC | ORG | WEAPON | EVENT | MISC
    start_char: int
    end_char: int
    confidence: float
    normalized: Optional[str] = None


@dataclass
class NLPResult:
    detected_language: str
    text_translated: Optional[str]
    threat_intent_score: float
    entities: List[NEREntity] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)


# Maps HuggingFace NER labels → NSOE ontology labels
LABEL_MAP = {
    "PER": "PER", "PERSON": "PER",
    "LOC": "LOC", "LOCATION": "LOC", "GPE": "LOC",
    "ORG": "ORG", "ORGANIZATION": "ORG",
    "WEAPON": "WEAPON", "WEA": "WEAPON",
    "EVENT": "EVENT",
}


class NLPPipeline:
    """
    Lazy-loads HuggingFace models on first use.
    Suitable for both batch and streaming (one-at-a-time) usage.
    """

    def __init__(self) -> None:
        self._lang_detector = None
        self._ner_pipeline = None
        self._intent_classifier = None
        self._translator = None
        self._loaded = False

    def load(self) -> None:
        from langdetect import detect as _detect
        from transformers import pipeline as hf_pipeline

        logger.info("nlp_pipeline_loading")

        self._lang_detector = _detect

        self._ner_pipeline = hf_pipeline(
            "ner",
            model=NER_MODEL_NAME,
            aggregation_strategy="simple",
            device=0 if DEVICE == "cuda" else -1,
        )

        self._intent_classifier = hf_pipeline(
            "text-classification",
            model=INTENT_MODEL_NAME,
            device=0 if DEVICE == "cuda" else -1,
        )

        # Translator is large — only load if env var set
        if os.environ.get("LOAD_TRANSLATOR", "false").lower() == "true":
            from transformers import MBartForConditionalGeneration, MBart50TokenizerFast
            self._translator_model = MBartForConditionalGeneration.from_pretrained(TRANSLATE_MODEL)
            self._translator_tok   = MBart50TokenizerFast.from_pretrained(TRANSLATE_MODEL)

        self._loaded = True
        logger.info("nlp_pipeline_ready")

    def process(self, text: str) -> NLPResult:
        if not self._loaded:
            self.load()

        # 1. Language detection
        try:
            lang = self._lang_detector(text[:500])
        except Exception:
            lang = "en"

        # 2. Translation to English if non-English
        text_translated: Optional[str] = None
        processing_text = text
        if lang != "en" and self._translator is not None:
            text_translated = self._translate(text, src_lang=lang)
            processing_text = text_translated or text

        # 3. NER
        entities = self._run_ner(processing_text[:MAX_LEN])

        # 4. Threat intent classification
        intent_score = self._run_intent(processing_text[:MAX_LEN])

        # 5. Keyword extraction (simple TF-IDF approximation)
        keywords = self._extract_keywords(processing_text)

        return NLPResult(
            detected_language=lang,
            text_translated=text_translated,
            threat_intent_score=intent_score,
            entities=entities,
            keywords=keywords,
        )

    def _run_ner(self, text: str) -> List[NEREntity]:
        if not self._ner_pipeline:
            return []
        try:
            raw_entities = self._ner_pipeline(text)
        except Exception as e:
            logger.warning("ner_error", error=str(e))
            return []
        entities = []
        for ent in raw_entities:
            label = LABEL_MAP.get(ent.get("entity_group", ""), "MISC")
            entities.append(NEREntity(
                text=ent.get("word", ""),
                label=label,
                start_char=ent.get("start", 0),
                end_char=ent.get("end", 0),
                confidence=float(ent.get("score", 0.0)),
            ))
        return entities

    def _run_intent(self, text: str) -> float:
        if not self._intent_classifier:
            return 0.0
        try:
            results = self._intent_classifier(text, truncation=True, max_length=MAX_LEN)
            # Assume binary classification: label 1 = threat
            for r in results:
                if "threat" in r["label"].lower() or r["label"] in ("LABEL_1", "1"):
                    return float(r["score"])
                if r["label"] in ("LABEL_0", "0"):
                    return 1.0 - float(r["score"])
            return float(results[0]["score"])
        except Exception as e:
            logger.warning("intent_error", error=str(e))
            return 0.0

    def _translate(self, text: str, src_lang: str) -> Optional[str]:
        try:
            from transformers import MBart50TokenizerFast
            lang_code = _mbart_lang_code(src_lang)
            if not lang_code:
                return None
            self._translator_tok.src_lang = lang_code
            encoded = self._translator_tok(
                text[:MAX_LEN], return_tensors="pt", truncation=True
            )
            generated = self._translator_model.generate(
                **encoded,
                forced_bos_token_id=self._translator_tok.lang_code_to_id["en_XX"],
            )
            return self._translator_tok.batch_decode(generated, skip_special_tokens=True)[0]
        except Exception as e:
            logger.debug("translation_error", error=str(e))
            return None

    def _extract_keywords(self, text: str) -> List[str]:
        import re
        # Simple: extract proper nouns and threat-related terms
        threat_terms = {
            "bomb", "attack", "blast", "shoot", "kill", "terror", "militant",
            "explosive", "grenade", "gun", "weapon", "kidnap", "ransom",
            "jihad", "ied", "threat", "threat", "assassination", "sedition",
        }
        words = re.findall(r"\b[A-Za-z]{3,}\b", text)
        found = {w.lower() for w in words if w.lower() in threat_terms}
        # Also grab capitalised words (likely proper nouns)
        caps = {w for w in words if w[0].isupper() and len(w) > 3}
        return list(found | set(list(caps)[:10]))


def _mbart_lang_code(lang: str) -> Optional[str]:
    """Map ISO 639-1 code to mBART-50 language token."""
    mapping = {
        "hi": "hi_IN", "ur": "ur_PK", "bn": "bn_IN",
        "pa": "pa_IN", "mr": "mr_IN", "ta": "ta_IN",
        "te": "te_IN", "kn": "kn_IN", "ml": "ml_IN",
        "gu": "gu_IN", "ar": "ar_AR", "fa": "fa_IR",
        "zh": "zh_CN", "fr": "fr_XX", "de": "de_DE",
        "es": "es_XX", "ru": "ru_RU",
    }
    return mapping.get(lang)

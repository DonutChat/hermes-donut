"""Local language detection for display text; never translate commands or model output."""
from functools import lru_cache
import re

from langdetect import DetectorFactory, LangDetectException
from langdetect.detector_factory import PROFILES_DIRECTORY

# Only the user's own prose is evidence. Quoted thread roots, URLs and code are not.
_NON_PROSE = re.compile(r"```[\s\S]*?(?:```|$)|`[^`\n]*`|https?://\S+|(?m:^\s*>.*$)|@\S+")


@lru_cache(maxsize=1)
def _factory():
    factory = DetectorFactory()
    factory.load_profile(PROFILES_DIRECTORY)
    factory.seed = 0  # repeatable decisions, with a fresh detector for each call
    return factory


def detect_language(text: str) -> str | None:
    """Return a confident language, or None for short/ambiguous/code-only messages.

    Hangul and kana also identify short conversational requests reliably. Longer
    prose uses the local statistical detector; no user text leaves the gateway.
    """
    if text.lstrip().startswith("/"):
        return None
    prose = _NON_PROSE.sub(" ", text[:8000])
    hangul = len(re.findall(r"[\uac00-\ud7a3\u1100-\u11ff\u3130-\u318f]", prose))
    kana = len(re.findall(r"[\u3040-\u309f\u30a0-\u30ff]", prose))
    if hangul >= 2 and not kana:
        return "ko"
    if kana >= 2 and not hangul:
        return "ja"
    if sum(c.isalpha() for c in prose) < 12:
        return None
    detector = _factory().create()
    detector.append(prose)
    try:
        choices = detector.get_probabilities()
    except LangDetectException:
        return None
    return choices[0].lang if choices and choices[0].prob >= 0.90 else None

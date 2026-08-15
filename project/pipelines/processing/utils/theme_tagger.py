"""Rule-based theme tagging for tweets.

The market-research study is about an AI video-analysis app for football, so
the "themes" are content topics (not clubs). They come straight from the KPI
spec: video analysis, stats/analytics and VAR/refereeing.

Matching is intentionally simple and transparent (a tunable keyword map) so a
junior can read and extend it. A sentiment model comes in a later iteration;
this module stays pure Python and has no Spark dependency, which keeps it fast
to unit-test offline.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Dict, List, Optional, Sequence

# Theme -> list of keywords. Order matters: it drives the order of the themes
# returned by ``tag_themes``. Edit these lists to tune precision/recall.
THEME_KEYWORDS: Dict[str, List[str]] = {
    "analyse_video": [
        "analyse video",
        "video analysis",
        "analyse d'image",
        "ralenti",
        "montage video",
        "highlights",
        "resume video",
        "sequence video",
    ],
    "stats_analytics": [
        "stats",
        "statistiques",
        "statistique",
        "analytics",
        "xg",
        "expected goals",
        "opta",
        "heatmap",
        "data foot",
        "datascout",
        "metriques",
    ],
    "var": [
        "var",
        "arbitrage",
        "arbitre",
        "hors jeu",
        "penalty",
        "carton rouge",
    ],
}


def _normalize(text: str) -> str:
    """Lowercase and strip accents so matching is accent-insensitive.

    Parameters
    ----------
    text : str
        Any input text.

    Returns
    -------
    str
        Lowercased, accent-free version (e.g. ``"vidéo"`` -> ``"video"``).
    """
    decomposed = unicodedata.normalize("NFKD", text)
    without_accents = "".join(c for c in decomposed if not unicodedata.combining(c))
    return without_accents.lower()


def _matches(
    keywords: Sequence[str],
    text_norm: str,
    hashtags_norm: Sequence[str]
) -> bool:
    """Return True if any keyword matches the text or a hashtag.

    Free text is matched on word boundaries (so ``var`` does not match
    ``varie``). Hashtags are matched on a compact, separator-free form so a
    camelCase hashtag like ``#AnalyseVideo`` still matches ``"analyse video"``.

    Parameters
    ----------
    keywords : Sequence[str]
        The theme's keywords (raw, will be normalized here).
    text_norm : str
        Already-normalized tweet text.
    hashtags_norm : Sequence[str]
        Already-normalized hashtags.

    Returns
    -------
    bool
        Whether the theme applies.
    """
    for keyword in keywords:
        kw_norm = _normalize(keyword)
        if re.search(rf"\b{re.escape(kw_norm)}\b", text_norm):
            return True
        compact = re.sub(r"[\s-]", "", kw_norm)
        if compact and any(compact in re.sub(r"-", "", h) for h in hashtags_norm):
            return True
    return False


def tag_themes(
    text: Optional[str],
    hashtags: Optional[Sequence[str]],
) -> List[str]:
    """Tag a tweet with the content themes it mentions.

    Parameters
    ----------
    text : Optional[str]
        The tweet text (``None`` is treated as empty).
    hashtags : Optional[Sequence[str]]
        The tweet's hashtags without the leading ``#`` (``None`` -> empty).

    Returns
    -------
    List[str]
        The matched themes in ``THEME_KEYWORDS`` declaration order. Empty list
        (never ``None``) when the tweet is off-topic.
    """
    text_norm = _normalize(text or "")
    hashtags_norm = [_normalize(h) for h in (hashtags or []) if h]
    return [
        theme
        for theme, keywords in THEME_KEYWORDS.items()
        if _matches(keywords, text_norm, hashtags_norm)
    ]

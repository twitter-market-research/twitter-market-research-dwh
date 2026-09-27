"""Rule-based theme tagging for tweets.

The study is about football conversation on Ligue 1. Themes are content
topics (not clubs), and the catalogue below is the single source of truth:
it drives the tagging *and* generates the ingestion search query, so the
corpus always contains what the dashboard offers.

Matching is intentionally simple and transparent (a tunable keyword map) so
a junior can read and extend it. A sentiment model comes in a later
iteration; this module stays pure Python and has no Spark dependency, which
keeps it fast to unit-test offline.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Dict, List, Optional, Sequence


# Contexte football ANDé aux termes de thème dans la requête d'ingestion.
# Sans lui, un mot ambigu comme "var" ramènerait n'importe quoi.
LIGUE1_CONTEXT: List[str] = [
    "Ligue1", '"Ligue 1"', "PSG", "OM", "OL", "LOSC", "ASSE",
]

# Theme -> keywords, most discriminating first. The leading terms of each
# theme feed the ingestion query (see build_search_query), the full list
# drives the tagging. Editing this dict is the only place a theme changes:
# the search query is derived from it, never written by hand.
THEME_KEYWORDS: Dict[str, List[str]] = {
    "stats_analytics": [
        "xg", "expected goals", "stats",
        "statistiques", "statistique", "analytics",
        "opta", "heatmap", "data foot", "metriques",
    ],
    "var": [
        "var", "arbitrage", "hors jeu",
        "arbitre", "penalty", "carton rouge", "carton jaune",
    ],
    "mercato": [
        "mercato", "transfert", "recrue",
        "transferts", "recrutement", "signature", "prolongation",
    ],
    "tactique": [
        "tactique", "pressing", "composition",
        "compo", "systeme", "dispositif", "bloc bas", "schema",
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
    camelCase hashtag like ``#HorsJeu`` still matches ``"hors jeu"``.

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


def build_search_query(
    context: Sequence[str],
    terms_per_theme: int = 3,
) -> str:
    """Build the X API v2 search query from the theme catalogue.

    Collection and tagging must never drift apart: the query is derived
    from THEME_KEYWORDS instead of being maintained by hand. Only the
    leading terms of each theme are used -- the X API caps a query at
    about 512 characters.

    Parameters
    ----------
    context : Sequence[str]
        Football context terms ANDed with the theme terms (clubs,
        competition), to keep ambiguous words like "var" on topic.
    terms_per_theme : int
        How many leading keywords to take per theme.

    Returns
    -------
    str
        A query such as ``("xg" OR ... ) (Ligue1 OR PSG OR ...)``.
    """
    terms = [
        f'"{kw}"' if " " in kw else kw
        for keywords in THEME_KEYWORDS.values()
        for kw in keywords[:terms_per_theme]
    ]
    return f"({' OR '.join(terms)}) ({' OR '.join(context)})"

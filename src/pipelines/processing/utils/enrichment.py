"""Per-tweet enrichment: engagement + content themes.

Pure Python, no Spark dependency. The Spark Structured Streaming job calls
``enrich`` per record (via a UDF / mapping); keeping the logic here means it is
unit-tested offline and reused unchanged if the runtime ever changes.
"""
from __future__ import annotations

from typing import Any, Dict

from project.pipelines.processing.utils.theme_tagger import tag_themes


def compute_engagement(record: Dict[str, Any]) -> int:
    """Compute a tweet's engagement.

    ``engagement = like_count + retweet_count`` (KPI #2 and #4). Missing or
    ``None`` counters are treated as ``0``; string counters are coerced.

    Parameters
    ----------
    record : Dict[str, Any]
        A flattened tweet (as produced by the ingestion serializer).

    Returns
    -------
    int
        The engagement score.
    """
    likes = int(record.get("like_count") or 0)
    retweets = int(record.get("retweet_count") or 0)
    return likes + retweets


def enrich(record: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of ``record`` with ``engagement`` and ``themes`` added.

    The input dict is not mutated.

    Parameters
    ----------
    record : Dict[str, Any]
        A flattened tweet with at least ``text`` and (optionally) ``hashtags``,
        ``like_count`` and ``retweet_count``.

    Returns
    -------
    Dict[str, Any]
        The enriched tweet, ready to be written to the staging table.
    """
    enriched = dict(record)
    enriched["engagement"] = compute_engagement(record)
    enriched["themes"] = tag_themes(record.get("text"), record.get("hashtags"))
    return enriched

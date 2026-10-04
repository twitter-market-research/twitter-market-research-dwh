"""
Field-level data contract for silver layer

Governance is only real if a violation breaks the build.
This single source of truth for what may reach the staging table: transform.py
derives its projection from it, so no column can reach silver layer without
being classified first, and tests/test_data_contract.py refuses any field whose
name appears on the personal-data denylist.
"""

from __future__ import annotations
from typing import NamedTuple, List


NON_PERSONAL = "non_personal"
PSEUDONYMOUS = "pseudonymous"
PERSONAL = "personal"
DERIVED = "derived"
CLASSES = frozenset({NON_PERSONAL, PSEUDONYMOUS, PERSONAL, DERIVED})


class Field(NamedTuple):
    """
    This class is a single source of truth for what may reach the staging
    table.

    Parameters
    ----------
    name : str
        Column name, as written to BigQuery
    data_class : str
        One of the module-level class constants.
    purpose : str
        Why the study needs it. A field with no purpose is one to drop.
    """

    name: str
    data_class: str
    purpose: str


# Contract order IS THE staging projection order
STAGING_CONTRACT: List[Field] = [
    Field("tweet_id", PSEUDONYMOUS, "dedup key, Kafka compaction key"),
    Field("author_id", PSEUDONYMOUS, "unique-author counts, engagement"),
    Field("lang", NON_PERSONAL, "corpus filter"),
    Field("text", PERSONAL, "theme tagging, future sentiment"),
    Field("hashtags", PERSONAL, "theme tagging"),
    Field("like_count", NON_PERSONAL, "engagement metric"),
    Field("retweet_count", NON_PERSONAL, "engagement metric"),
    Field("reply_count", NON_PERSONAL, "engagement metric"),
    Field("themes", DERIVED, "dashboard filter"),
    Field("created_at", NON_PERSONAL, "time_series"),
    Field("processed_at", NON_PERSONAL, "pipeline lineage")
]


DENYLIST: frozenset[str] = frozenset({
    "username",
    "name",
    "screen_name",
    "description",
    "bio",
    "location",
    "profile_image_url",
    "raw_payload",
    }
)

# Retention will be applied to every store holding these fields
RETENTION_DAYS = 90


def staging_columns() -> List[str]:
    """
    Return the staging projection, in contract order.

    Returns
    -------
    List[str]
        Column names, for the silver ''select''.
    """
    return [field.name for field in STAGING_CONTRACT]

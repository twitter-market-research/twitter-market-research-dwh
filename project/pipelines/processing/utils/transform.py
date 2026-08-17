"""Streaming transform: raw Kafka rows -> enriched analytical rows.

The theme tagging reuses the pure ``tag_themes`` rules inside a Spark UDF, so
the exact logic unit-tested in Phase 1 runs in the distributed job. Engagement
is a plain column expression (no Python round-trip). ``enrich_stream`` works on
any DataFrame that has a ``value`` column, so it can be exercised on a small
static DataFrame in tests as well as on the live Kafka stream.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from project.pipelines.processing.utils.schema import TWEET_SCHEMA
from project.pipelines.processing.utils.theme_tagger import tag_themes

# Spark UDF wrapping the pure tag_themes() so the tested rules run per row.
_tag_themes_udf = F.udf(
    lambda text, hashtags: tag_themes(text, hashtags),
    ArrayType(StringType()),
)

# Final projection written to the staging table (stable column order).
_STAGING_COLUMNS = [
    "tweet_id",
    "author_id",
    "lang",
    "text",
    "hashtags",
    "like_count",
    "retweet_count",
    "engagement",
    "themes",
    "created_at",
    "processed_at",
]


def enrich_stream(raw: DataFrame) -> DataFrame:
    """Parse ``tweets_raw`` Kafka rows and add the analytical columns.

    Parameters
    ----------
    raw : DataFrame
        DataFrame with a ``value`` column holding the JSON payload (binary or
        string). In production this is the streaming DataFrame read from Kafka.

    Returns
    -------
    DataFrame
        One row per tweet with the flattened fields plus ``engagement``,
        ``themes`` (array), ``created_at`` (timestamp) and ``processed_at``.
        Rows whose ``tweet_id`` is null (unparseable payloads) are dropped.
    """
    parsed = (
        raw
        .select(
            F.from_json(F.col("value").cast("string"), TWEET_SCHEMA).alias("t")
        )
        .select("t.*")
        .where(F.col("tweet_id").isNotNull())
    )
    return (
        parsed
        .withColumn(
            "engagement",
            F.coalesce(F.col("like_count"), F.lit(0))
            + F.coalesce(F.col("retweet_count"), F.lit(0)),
        )
        .withColumn(
            "themes",
            _tag_themes_udf(F.col("text"), F.col("hashtags")),
        )
        .withColumn("created_at", F.to_timestamp(F.col("created_at")))
        .withColumn("processed_at", F.current_timestamp())
        .select(*_STAGING_COLUMNS)
    )

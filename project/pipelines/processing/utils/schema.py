"""Explicit Spark schema for the ``tweets_raw`` Kafka messages.

Kafka delivers each tweet as a raw JSON string. To turn that string
into real, typed columns we call ``from_json(value, schema)`` -- and
this ``StructType`` is that schema. Declaring it by hand (instead of
letting Spark infer it) is the right practice in streaming: it is
deterministic, fast, and a malformed record yields ``null`` fields
instead of crashing the whole stream.

It mirrors the flattened shape produced by the ingestion serializer
(``TweetSerializer.serialize``). ``raw_payload`` is intentionally
omitted: the staging table only needs the analytical fields.
"""
from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# One StructField per column (name + type)
TWEET_SCHEMA = StructType([
    StructField("tweet_id", StringType(), nullable=True),
    StructField("text", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("author_id", StringType(), nullable=True),
    StructField("lang", StringType(), nullable=True),
    StructField("like_count", LongType(), nullable=True),
    StructField("retweet_count", LongType(), nullable=True),
    StructField("hashtags", ArrayType(StringType()), nullable=True),
])

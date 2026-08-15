"""
"""

from datetime import datetime
import json
import pytest

pytest.importorskip("pyspark")

from project.pipelines.processing.utils.transform import ( # noqa: E402.
    enrich_stream
)
from pyspark.sql import SparkSession


# A well-formed tweet message, as produced by thne ingestion layer (TweetSerializer)

VALID_TWEET = {
    "tweet_id": "20",
    "text": "This is a test tweet",
    "created_at": "2026-08-14T10:00:00.000Z",
    "author_id": "43",
    "lang": "fr",
    "like_count": 10,
    "retweet_count": 5,
    "hashtags": ["test", "tweet"]
}

@pytest.fixture(scope="module")
def spark():
    """Local single-threaded SparkSession for testing, or skip if no JVM is available."""

    try:
        session = (
            SparkSession.builder
            .master("local[1]")
            .appName("test-transform")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as e:
        # any JVM failure means skip , not fail
        pytest.skip(f"Could not create SparkSession: {e}")

    yield session
    session.stop()


def _frame(spark, *payloads: str):
    """Build a one-column ''value'' DtataFrame from a list of JSON strings"""
    return spark.createDataFrame([(p,) for p in payloads], "value string")


class TestEnrichStream:
    """
    Test the ``enrich_stream`` function, which transforms a raw Kafka stream of
    JSON strings into a typed DataFrame with analytical columns.
    """

    def test_valid_tweet(self, spark) -> None:
        """A valid tweet should be parsed into a row with the right values."""
        df = _frame(spark, json.dumps(VALID_TWEET))
        enriched = enrich_stream(df)
        row = enriched.collect()[0]
        expected_created_at = datetime.strptime(
            VALID_TWEET["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
        )

        assert row.tweet_id == VALID_TWEET["tweet_id"]
        assert row.text == VALID_TWEET["text"]
        assert row.created_at == expected_created_at
        assert row.author_id == VALID_TWEET["author_id"]
        assert row.lang == VALID_TWEET["lang"]
        assert row.like_count == VALID_TWEET["like_count"]
        assert row.retweet_count == VALID_TWEET["retweet_count"]
        assert row.hashtags == VALID_TWEET["hashtags"]

    def test_drops_unparseable_payloads(self, spark) -> None:
        """A malformed payloads  should be dropped from the output DataFrame."""
        dataframe = _frame(spark, "{not a json}", json.dumps(VALID_TWEET))
        enriched = enrich_stream(dataframe)
        rows_to_drop = enriched.collect()

        assert len(rows_to_drop) == 1
        assert rows_to_drop[0].tweet_id == VALID_TWEET["tweet_id"]

    def test_computes_engagement_score(self, spark) -> None:
        """engagement = like_count + retweet_count"""

        dataframe = _frame(spark, json.dumps(VALID_TWEET))
        enriched = enrich_stream(dataframe)
        assert enriched.collect()[0]["engagement"] == VALID_TWEET["like_count"] + VALID_TWEET["retweet_count"]

    def test_null_counters_default_to_zero(self, spark) -> None:
        """
        Missing metrics must not null out the whole engagement column"
        """
        payload = dict(VALID_TWEET)
        del payload["like_count"]
        del payload["retweet_count"]
        dataframe = _frame(spark, json.dumps(payload))
        assert enrich_stream(dataframe).collect()[0]["engagement"] == 0

    def test_tag_themes(
        self,
        spark,
    ) -> None:
        payload = dict(VALID_TWEET, text="This is a test tweet about var and other things")
        dataframe = _frame(spark, json.dumps(payload))
        assert enrich_stream(dataframe).collect()[0]["themes"] == ["var"]

    def test_offtopic_tweet_has_no_theme(self, spark) -> None:
        """VALID_TWEET is deliberately off-topic: no theme must match."""
        dataframe = _frame(spark, json.dumps(VALID_TWEET))
        assert enrich_stream(dataframe).collect()[0]["themes"] == []

    def test_projects_the_staging_columns(self, spark) -> None:
        """The output DataFrame should only contain the columns needed for the staging table."""
        dataframe = _frame(spark, json.dumps(VALID_TWEET))
        enriched = enrich_stream(dataframe)
        assert set(enriched.columns) == {
            "tweet_id",
            "text",
            "created_at",
            "author_id",
            "lang",
            "like_count",
            "retweet_count",
            "hashtags",
            "engagement",
            "themes",
            "processed_at"
        }
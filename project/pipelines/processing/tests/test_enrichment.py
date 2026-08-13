from project.pipelines.processing.utils.enrichment import (
    compute_engagement,
    enrich,
)


class TestComputeEngagement:
    """engagement = like_count + retweet_count (KPI #2 / #4)."""

    def test_sums_likes_and_retweets(self) -> None:
        assert compute_engagement({"like_count": 12, "retweet_count": 4}) == 16

    def test_missing_metrics_default_to_zero(self) -> None:
        assert compute_engagement({}) == 0

    def test_none_metrics_treated_as_zero(self) -> None:
        record = {"like_count": None, "retweet_count": 5}
        assert compute_engagement(record) == 5

    def test_string_metrics_are_coerced(self) -> None:
        """Metrics arriving as strings are coerced to int."""
        record = {"like_count": "10", "retweet_count": "3"}
        assert compute_engagement(record) == 13


class TestEnrich:
    """Full per-tweet enrichment: adds engagement + themes, keeps input."""

    def test_adds_engagement_and_themes(self) -> None:
        record = {
            "tweet_id": "1",
            "text": "Decision VAR scandaleuse",
            "hashtags": ["VAR"],
            "like_count": 8,
            "retweet_count": 2,
        }
        out = enrich(record)
        assert out["engagement"] == 10
        assert out["themes"] == ["var"]

    def test_preserves_original_fields(self) -> None:
        record = {"tweet_id": "42", "text": "hello", "hashtags": []}
        out = enrich(record)
        assert out["tweet_id"] == "42"
        assert out["text"] == "hello"

    def test_does_not_mutate_input(self) -> None:
        record = {"tweet_id": "1", "text": "hi", "hashtags": []}
        enrich(record)
        assert "engagement" not in record
        assert "themes" not in record

    def test_offtopic_tweet_has_empty_themes(self) -> None:
        record = {"tweet_id": "1", "text": "Beau but !", "hashtags": []}
        assert enrich(record)["themes"] == []

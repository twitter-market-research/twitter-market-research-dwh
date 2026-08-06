from project.pipelines.ingestion.tweets_raw.utils.key_builder import (
    KeyBuilder,
)


class TestKeyBuilder:

    def test_key_is_author_id(self, valid_tweet: dict) -> None:
        """La clé Kafka est l'author_id (répartition équilibrée)."""
        assert KeyBuilder().build(valid_tweet) == b"99887766"

    def test_key_is_bytes(self, valid_tweet: dict) -> None:
        """La clé retournée est toujours de type bytes."""
        assert isinstance(KeyBuilder().build(valid_tweet), bytes)

    def test_falls_back_to_tweet_id_when_no_author(self) -> None:
        """Sans author_id, on se rabat sur l'id du tweet."""
        tweet = {"id": "42"}
        assert KeyBuilder().build(tweet) == b"42"

    def test_fallback_unknown_when_no_author_nor_id(self) -> None:
        """Sans author_id ni id, la clé est b"unknown"."""
        assert KeyBuilder().build({}) == b"unknown"

    def test_same_author_same_key(self) -> None:
        """Deux tweets du même auteur produisent la même clé (co-location)."""
        builder = KeyBuilder()
        a = builder.build({"author_id": "123", "id": "1"})
        b = builder.build({"author_id": "123", "id": "2"})
        assert a == b

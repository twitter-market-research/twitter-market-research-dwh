"""KeyBuilder : la clé Kafka d'un tweet est son author_id.

Le club n'est plus la clé — il reste dans les hashtags de la valeur du
message, pour l'agrégation en aval.
"""
from project.pipelines.ingestion.tweets_raw.utils.key_builder import (
    FALLBACK_KEY,
    KeyBuilder,
)


class TestKeyBuilder:
    """Clé de partition : author_id, avec repli explicite."""

    def test_key_is_the_author_id(self) -> None:
        assert KeyBuilder().build({"id": "1", "author_id": "2003"}) == b"2003"

    def test_key_is_bytes(self) -> None:
        assert isinstance(KeyBuilder().build({"author_id": "42"}), bytes)

    def test_numeric_author_id_is_coerced(self) -> None:
        """L'API renvoie une chaîne, mais un int ne doit pas casser la clé."""
        assert KeyBuilder().build({"author_id": 2003}) == b"2003"

    def test_fallback_when_author_id_missing(self) -> None:
        assert KeyBuilder().build({"id": "1"}) == FALLBACK_KEY

    def test_fallback_when_author_id_empty(self) -> None:
        assert KeyBuilder().build({"author_id": ""}) == FALLBACK_KEY

    def test_hashtags_do_not_influence_the_key(self) -> None:
        """Régression : la clé ne dépend plus des hashtags de club."""
        tweet = {
            "author_id": "2003",
            "entities": {"hashtags": [{"tag": "PSG"}, {"tag": "OM"}]},
        }
        assert KeyBuilder().build(tweet) == b"2003"

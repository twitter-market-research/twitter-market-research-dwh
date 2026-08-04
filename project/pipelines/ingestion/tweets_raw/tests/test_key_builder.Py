from project.pipelines.ingestion.tweets_raw.utils.key_builder import (
    KeyBuilder,
)


def _with_hashtags(tweet: dict, tags: list) -> dict:
    """Renvoie une copie du tweet dont entities.hashtags porte ``tags``.

    Parameters
    ----------
    tweet : dict
        Tweet de base (schéma X API v2).
    tags : list
        Liste de tags (chaînes) à injecter dans entities.hashtags.

    Returns
    -------
    dict
        Copie du tweet avec le schéma imbriqué entities.hashtags renseigné.
    """
    clone = dict(tweet)
    clone["entities"] = {"hashtags": [{"tag": t} for t in tags]}
    clone.pop("hashtags", None)
    return clone


class TestKeyBuilder:

    def test_extracts_club_from_entities_psg(self, valid_tweet: dict) -> None:
        """PSG présent dans entities.hashtags → clé b"PSG"."""
        tweet = _with_hashtags(valid_tweet, ["Ligue1", "PSG"])
        assert KeyBuilder().build(tweet) == b"PSG"

    def test_extracts_club_from_entities_om(self, valid_tweet: dict) -> None:
        """OM présent dans entities.hashtags → clé b"OM"."""
        tweet = _with_hashtags(valid_tweet, ["Ligue1", "OM"])
        assert KeyBuilder().build(tweet) == b"OM"

    def test_key_is_bytes(self, valid_tweet: dict) -> None:
        """La clé retournée est toujours de type bytes."""
        assert isinstance(KeyBuilder().build(valid_tweet), bytes)

    def test_detects_club_case_insensitive(self, valid_tweet: dict) -> None:
        """La détection du club est insensible à la casse."""
        tweet = _with_hashtags(valid_tweet, ["ligue1", "ogcnice"])
        assert KeyBuilder().build(tweet) == b"OGCNICE"

    def test_fallback_ligue1_when_no_club_found(
        self, valid_tweet: dict
    ) -> None:
        """Repli sur b"Ligue1" si aucun hashtag ne matche un club."""
        tweet = _with_hashtags(valid_tweet, ["football", "VAR"])
        assert KeyBuilder().build(tweet) == b"Ligue1"

    def test_fallback_ligue1_when_empty_hashtags(
        self, valid_tweet: dict
    ) -> None:
        """Repli sur b"Ligue1" si entities.hashtags est vide."""
        tweet = _with_hashtags(valid_tweet, [])
        assert KeyBuilder().build(tweet) == b"Ligue1"

    def test_fallback_ligue1_when_entities_missing(
        self, valid_tweet: dict
    ) -> None:
        """Repli sur b"Ligue1" si le tweet n'a ni entities ni hashtags."""
        tweet = dict(valid_tweet)
        tweet.pop("entities", None)
        tweet.pop("hashtags", None)
        assert KeyBuilder().build(tweet) == b"Ligue1"

    def test_first_club_wins_when_multiple(self, valid_tweet: dict) -> None:
        """Le premier club rencontré l'emporte si plusieurs sont présents."""
        tweet = _with_hashtags(valid_tweet, ["PSG", "OM", "Ligue1"])
        assert KeyBuilder().build(tweet) == b"PSG"

    def test_legacy_flat_hashtags_still_supported(self) -> None:
        """Compat : l'ancien schéma plat ``hashtags`` est lu si entities
        est absent."""
        tweet = {"id": "1", "hashtags": ["Ligue1", "OM"]}
        assert KeyBuilder().build(tweet) == b"OM"

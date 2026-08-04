import json
import pytest
from project.pipelines.ingestion.tweets_raw.utils.tweet_serializer import TweetSerializer


class TestTweetSerializer:

    def test_serialize_returns_bytes(
        self,
        valid_tweet: dict
    ):
        """
        This test checks that the serialize method returns bytes.
        Args:
            valid_tweet (dict): A fixture that provides a
             valid tweet dictionary.
        """
        serializer = TweetSerializer()
        serialized = serializer.serialize(valid_tweet)
        assert isinstance(serialized, bytes)

    def test_seriazlize_returns_valid_json(
        self,
        valid_tweet: dict
    ):
        """
        This test checks that the serialize method returns valid JSON.
        Args:
            valid_tweet (dict): A fixture that provides a
             valid tweet dictionary.
        """
        serializer = TweetSerializer()
        serialized = serializer.serialize(valid_tweet)
        deserialized = json.loads(serialized)
        assert isinstance(deserialized, dict)

    def test_deserialize_round_trips_serialized_payload(
        self,
        valid_tweet: dict
    ):
        """Vérifie que deserialize est bien l'inverse de l'encodage JSON.

        serialize remodèle le tweet (aplatissement + raw_payload), donc
        l'aller-retour ne redonne pas le tweet ORIGINAL mais bien la
        structure produite par serialize. L'original reste accessible sous
        ``raw_payload``.

        Parameters
        ----------
        valid_tweet : dict
            Fixture d'un tweet valide (schéma X API v2).
        """
        serializer = TweetSerializer()
        serialized = serializer.serialize(valid_tweet)

        deserialized = serializer.deserialize(serialized)

        assert deserialized == json.loads(serialized.decode("utf-8"))
        assert deserialized["tweet_id"] == valid_tweet["id"]
        assert deserialized["raw_payload"] == valid_tweet

    def test_serialize_none_raises_value_error(
        self
    ):
        """
        This test checks that the serialize method raises a ValueError
         when passed None.
        """
        serializer = TweetSerializer()
        with pytest.raises(ValueError):
            serializer.serialize(None)
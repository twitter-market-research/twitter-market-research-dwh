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

    def test_deserialize_returns_original_dict(
        self,
        valid_tweet: dict
    ):
        """
        This test checks that the deserialize method returns
         the original dictionary.
        Args:
            valid_tweet (dict): A fixture that provides a
             valid tweet dictionary.
        """
        serializer = TweetSerializer()
        serialized = serializer.serialize(valid_tweet)
        deserialized = serializer.deserialize(serialized)
        assert deserialized == valid_tweet

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
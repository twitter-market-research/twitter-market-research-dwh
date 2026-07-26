import json

from pydantic import ValidationError


class TweetSerializer:
    """
    This class serializes/deserializes a tweet dictionary ↔ bytes (JSON UTF-8).
    """

    def serialize(self, raw_tweet: dict) -> bytes:
        """"
        This function serializes a tweet dictionary to bytes (JSON UTF-8).
        Args:
            raw_tweet (dict): The raw tweet dictionary to serialize.
        Returns:
            bytes: The serialized tweet as bytes.
        """
        # 1. tweet_id → vient de 'id'
        tweet_id = raw_tweet.get("id")
        if not tweet_id:
            raise ValidationError("Champ manquant : tweet_id")

        # 2. Texte du tweet
        text = raw_tweet.get("text", "")

        # 3. like_count → vient de public_metrics.like_count
        public_metrics = raw_tweet.get("public_metrics", {})
        like_count = public_metrics.get("like_count", 0)

        # 4. retweet_count → vient de public_metrics.retweet_count
        retweet_count = public_metrics.get("retweet_count", 0)

        # 5. hashtags → à extraire de entities.hashtags[].tag
        entities = raw_tweet.get("entities", {})
        hashtags_list = entities.get("hashtags", [])
        hashtags = [h.get("tag") for h in hashtags_list if h.get("tag")]

        # 6. created_at
        created_at = raw_tweet.get("created_at", "")

        # 7. author_id
        author_id = raw_tweet.get("author_id", "")

        # 8. language
        lang = raw_tweet.get("lang", "unknown")

        return json.dumps({
            "tweet_id": tweet_id,
            "text": text,
            "created_at": created_at,
            "author_id": author_id,
            "lang": lang,
            "like_count": like_count,
            "retweet_count": retweet_count,
            "hashtags": hashtags,
            "raw_payload": raw_tweet,
        }, ensure_ascii=False).encode("utf-8")

    def deserialize(self, data: bytes) -> dict:
        """
        This function deserializes bytes (JSON UTF-8)
        back to a tweet dictionary.
        Args:
            data (bytes): The bytes to deserialize.
        Returns:
            dict: The deserialized tweet dictionary.
        """
        try:
            return json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            msg = f"Impossible de désérialiser les bytes : {e}"
            raise ValueError(msg) from e

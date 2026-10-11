import json


class TweetSerializer:
    """
    Sérialise/désérialise un tweet dict ↔ bytes (JSON UTF-8).

    ``serialize`` ne fait pas qu'encoder : il **remodèle** le tweet brut de
    l'API X v2 vers le schéma aplati attendu en aval (Kafka, S3), tout en
    conservant l'original sous ``raw_payload``.
    """

    def serialize(self, raw_tweet: dict) -> bytes:
        """
        Sérialise un tweet vers le schéma aplati (bytes JSON UTF-8).

        Parameters
        ----------
        raw_tweet : dict
            Tweet brut au schéma X API v2 (doit contenir un champ ``id``).

        Returns
        -------
        bytes
            Le tweet remodelé, encodé en JSON UTF-8.

        Raises
        ------
        ValueError
            Si l'entrée n'est pas un dict, ou si le champ ``id`` est absent.
        """
        if not isinstance(raw_tweet, dict):
            raise ValueError("Le tweet à sérialiser doit être un dict")

        # 1. tweet_id → vient de 'id'
        tweet_id = raw_tweet.get("id")
        if not tweet_id:
            raise ValueError("Champ manquant : id")

        # 2. Texte du tweet
        text = raw_tweet.get("text", "")

        # 3/4. Compteurs → depuis public_metrics
        public_metrics = raw_tweet.get("public_metrics", {})
        like_count = public_metrics.get("like_count", 0)
        retweet_count = public_metrics.get("retweet_count", 0)

        # 5. hashtags → à extraire de entities.hashtags[].tag
        entities = raw_tweet.get("entities", {})
        hashtags_list = entities.get("hashtags", [])
        hashtags = [h.get("tag") for h in hashtags_list if h.get("tag")]

        # 6/7/8. Autres champs
        created_at = raw_tweet.get("created_at", "")
        author_id = raw_tweet.get("author_id", "")
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
        Désérialise des bytes (JSON UTF-8) vers un dict.

        Parameters
        ----------
        data : bytes
            Charge utile JSON encodée en UTF-8.

        Returns
        -------
        dict
            Le tweet remodelé (tel que produit par ``serialize``).

        Raises
        ------
        ValueError
            Si les bytes ne sont pas du JSON UTF-8 valide.
        """
        try:
            return json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            msg = f"Impossible de désérialiser les bytes : {e}"
            raise ValueError(msg) from e

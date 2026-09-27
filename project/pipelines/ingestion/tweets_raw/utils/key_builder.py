from typing import Any, Dict

FALLBACK_KEY = b"unknown"


class KeyBuilder:
    """
    Construit la clé de partition Kafka d'un tweet.

    La clé est l'``author_id`` : elle répartit la charge de façon équilibrée
    entre les partitions (hash de l'auteur) tout en co-localisant les tweets
    d'un même auteur (utile pour la déduplication ou l'analyse par compte).
    Le club n'est plus la clé — il reste disponible dans les hashtags de la
    valeur du message pour l'agrégation en aval.
    """

    def build(self, tweet: Dict[str, Any]) -> bytes:
        """
        Construit la clé Kafka (bytes) d'un tweet : son ``author_id``.

        Parameters
        ----------
        tweet : Dict[str, Any]
            Tweet au schéma X API v2.

        Returns
        -------
        bytes
            L'``author_id`` encodé en UTF-8, ou ``b"unknown"`` s'il est
            absent ou vide.
        """
        author_id = tweet.get("author_id")
        if author_id:
            return str(author_id).encode("utf-8")
        return FALLBACK_KEY

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
        Construit la clé Kafka (bytes) d'un tweet.

        Parameters
        ----------
        tweet : Dict[str, Any]
            Tweet au schéma X API v2.

        Returns
        -------
        bytes
            L'``author_id`` encodé en UTF-8 ; à défaut l'``id`` du tweet ;
            et ``b"unknown"`` si aucun des deux n'est présent.
        """
        author_id = tweet.get("author_id") or tweet.get("id")
        if not author_id:
            return FALLBACK_KEY
        return str(author_id).encode("utf-8")

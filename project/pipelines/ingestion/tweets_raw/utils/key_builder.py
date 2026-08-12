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
        Construit une clé Kafka (bytes) à partir des hashtags d'un tweet.

        Parameters
        ----------
        tweet : Dict[str, Any]
            Tweet au schéma X API v2. Les hashtags sont lus dans
            ``entities.hashtags[].tag`` (schéma réel de l'API) ; un ancien
            schéma plat ``hashtags`` (liste de chaînes) reste supporté par
            compatibilité.

        Returns
        -------
        bytes
            Le nom du premier club Ligue 1 détecté (ex: ``b"OM"``), ou
            ``b"Ligue1"`` si aucun club n'est reconnu.
        """
        for tag in self._extract_tags(tweet):
            normalized = tag.lower().lstrip("#")
            club = CLUB_HASHTAGS.get(normalized)
            if club:
                return club.encode("utf-8")
        return FALLBACK_KEY

    def _extract_tags(self, tweet: Dict[str, Any]) -> list:
        """
        Extrait la liste des hashtags (chaînes) d'un tweet, quel que soit
        le schéma.

        Parameters
        ----------
        tweet : Dict[str, Any]
            Tweet pouvant contenir ``entities.hashtags[].tag`` (schéma X API
            v2) ou un champ plat ``hashtags`` (ancien schéma).

        Returns
        -------
        list
            Liste des tags (chaînes). Vide si aucun hashtag n'est présent.
        """
        entities = tweet.get("entities") or {}
        nested = entities.get("hashtags")
        if nested:
            return [
                h.get("tag", "")
                for h in nested
                if isinstance(h, dict) and h.get("tag")
            ]
        # Compat : ancien schéma plat (liste de chaînes).
        return tweet.get("hashtags", []) or []

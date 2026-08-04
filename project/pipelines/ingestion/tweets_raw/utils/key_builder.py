from typing import Dict, Any

# Mapping hashtag (lowercase) → clé Kafka normalisée
CLUB_HASHTAGS: Dict[str, str] = {
    "psg":            "PSG",
    "allez_paris":    "PSG",
    "om":             "OM",
    "allezlom":       "OM",
    "ol":             "OL",
    "teamol":         "OL",
    "ogcnice":        "OGCNICE",
    "lesgym":         "OGCNICE",
    "losc":           "LOSC",
    "asmonaco":       "MONACO",
    "asm":            "MONACO",
    "girondins":      "BORDEAUX",
    "fcnantes":       "NANTES",
    "rennais":        "RENNES",
    "srfc":           "RENNES",
    "rcstrasbourg":   "STRASBOURG",
    "rcsa":           "STRASBOURG",
    "estac":          "TROYES",
    "fclorient":      "LORIENT",
    "angers":         "ANGERS",
    "sco":            "ANGERS",
    "reims":          "REIMS",
    "staderennais":   "RENNES",
    "mhsc":           "MONTPELLIER",
    "toulouse":       "TOULOUSE",
    "tfc":            "TOULOUSE",
    "auxerre":        "AUXERRE",
    "aja":            "AUXERRE",
    "havre":          "HAVRE",
    "hac":            "HAVRE",
    "brest":          "BREST",
    "sbfcbrest":      "BREST",
    "lens":           "LENS",
    "rclens":         "LENS",
    "saintetienne":   "SAINTETIENNE",
    "asse":           "SAINTETIENNE",
}

FALLBACK_KEY = b"Ligue1"


class KeyBuilder:
    """
    This class builds a Kafka key from tweet hashtags.

    Returns the first Ligue 1 club detected in the hashtags,
    or b'Ligue1' if no club is recognized.
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

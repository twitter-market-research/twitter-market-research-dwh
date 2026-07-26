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
        This function builds a Kafka key (bytes) from the hashtags of a tweet.
        Args:
            tweet (Dict[str, Any]): The tweet dictionary containing
             a "hashtags" field.
        Returns:
            bytes: The Kafka key derived from the hashtags.
        """
        hashtags = tweet.get("hashtags", [])
        for tag in hashtags:
            normalized = tag.lower().lstrip("#")
            club = CLUB_HASHTAGS.get(normalized)
            if club:
                return club.encode("utf-8")
        return FALLBACK_KEY

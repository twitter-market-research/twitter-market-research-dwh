# tests/conftest.py
import pytest


@pytest.fixture
def valid_tweet():
    """Tweet au schéma X API v2 (imbriqué), tel que renvoyé par search_tweets().

    NB transition (dette #B) : le champ plat ``hashtags`` est dupliqué à côté de
    ``entities.hashtags`` pour que KeyBuilder (qui lit encore le champ plat)
    reste testable tant que sa migration vers ``entities.hashtags`` n'est pas
    faite. La donnée réelle de l'API ne contient QUE ``entities.hashtags``.
    """
    return {
        "id": "1891234567890",
        "text": "Encore une décision VAR incompréhensible... #Ligue1 #OM",
        "created_at": "2026-03-08T12:15:00Z",
        "lang": "fr",
        "author_id": "99887766",
        "public_metrics": {
            "like_count": 12,
            "retweet_count": 4,
            "reply_count": 2,
            "quote_count": 0,
        },
        "entities": {
            "hashtags": [
                {"tag": "Ligue1"},
                {"tag": "OM"},
                {"tag": "VAR"},
            ]
        },
        # --- champ plat de transition (dette #B) ---
        "hashtags": ["Ligue1", "OM", "VAR"],
    }


@pytest.fixture
def invalid_tweet_missing_fields():
    """Tweet incomplet : il manque created_at, lang, author_id, public_metrics."""
    return {
        "id": "1891234567890",
        "text": "Super match !",
        # created_at manquant, lang manquant, author_id manquant,
        # public_metrics manquant
    }

# tests/conftest.py
import pytest


@pytest.fixture
def valid_tweet():
    return {
        "tweet_id": "1891234567890",
        "text": "Encore une décision VAR incompréhensible...",
        "created_at": "2026-03-08T12:15:00Z",
        "lang": "fr",
        "author_id": "99887766",
        "author_username": "fan_om",
        "like_count": 12,
        "retweet_count": 4,
        "reply_count": 2,
        "quote_count": 0,
        "hashtags": ["Ligue1", "OM", "VAR"],
        "source_query": "#Ligue1 OR #OM OR VAR"
    }


@pytest.fixture
def invalid_tweet_missing_fields():
    return {
        "tweet_id": "1891234567890",
        "text": "Super match !",
        # created_at manquant, lang manquant
    }

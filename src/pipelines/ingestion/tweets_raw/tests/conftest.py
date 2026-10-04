# tests/conftest.py
import asyncio
import inspect

import pytest


# ─────────────────────────────────────────────────────────────────────
# Support des tests `async def` sans dépendance externe.
#
# L'environnement n'a pas `pytest-asyncio` (et pas d'accès réseau pour
# l'installer). Ce hook exécute toute fonction de test coroutine via
# `asyncio.run()`. À supprimer si `pytest-asyncio` est ajouté aux
# dépendances de dev.
# ─────────────────────────────────────────────────────────────────────

def pytest_configure(config):
    """Enregistre le marqueur `asyncio` pour éviter le warning inconnu."""
    config.addinivalue_line(
        "markers",
        "asyncio: exécute le test via asyncio.run (fallback sans "
        "pytest-asyncio)",
    )


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem):
    """Exécute les tests coroutine ; laisse pytest gérer les tests sync.

    Parameters
    ----------
    pyfuncitem : pytest.Function
        L'item de test que pytest s'apprête à appeler.

    Returns
    -------
    Optional[bool]
        True si le test coroutine a été exécuté ici (court-circuite
        l'appel standard), None sinon (test synchrone laissé à pytest).
    """
    func = pyfuncitem.obj
    if inspect.iscoroutinefunction(func):
        argnames = pyfuncitem._fixtureinfo.argnames
        kwargs = {name: pyfuncitem.funcargs[name] for name in argnames}
        asyncio.run(func(**kwargs))
        return True
    return None


@pytest.fixture
def valid_tweet():
    """Tweet au schéma X API v2 (imbriqué), tel que produit par l'API.

    Les hashtags sont dans ``entities.hashtags[].tag`` — le schéma réel de
    l'API X v2, désormais lu de bout en bout (Serializer et KeyBuilder).
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
    }


@pytest.fixture
def invalid_tweet_missing_fields():
    """Tweet incomplet : created_at, lang, author_id, metrics absents."""
    return {
        "id": "1891234567890",
        "text": "Super match !",
        # created_at manquant, lang manquant, author_id manquant,
        # public_metrics manquant
    }

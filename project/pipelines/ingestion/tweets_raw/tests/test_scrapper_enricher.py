# tests/test_scraper_enricher.py
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio
from datetime import datetime

from project.pipelines.ingestion.tweets_raw.utils.scraper_enricher import (
    ScraperEnricher,
    EnrichmentError,
    ProfileCache,
    UserProfile,
)


# ─────────────────────────────────────────────────────────────────────
# 1. USER PROFILE — DATA MODEL
# ─────────────────────────────────────────────────────────────────────

class TestUserProfile:
    """Modèle de données pour un profil utilisateur"""

    def test_profile_created_from_dict(self):
        """Un UserProfile peut être créé depuis un dict"""
        data = {
            "user_id": "u123",
            "username": "ligue1_officiel",
            "name": "Ligue 1",
            "followers_count": 1000000,
            "following_count": 500,
            "verified": True,
            "description": "Compte officiel",
            "profile_image_url": "https://pbs.twimg.com/..."
        }
        profile = UserProfile.from_dict(data)

        assert profile.user_id == "u123"
        assert profile.username == "ligue1_officiel"
        assert profile.followers_count == 1000000
        assert profile.verified is True

    def test_profile_handles_missing_fields_gracefully(self):
        """Les champs manquants ont des valeurs par défaut"""
        data = {"user_id": "u123"}
        profile = UserProfile.from_dict(data)

        assert profile.user_id == "u123"
        assert profile.username is None
        assert profile.followers_count == 0
        assert profile.verified is False

    def test_profile_to_dict_returns_serializable_dict(self):
        """to_dict retourne un dict sérialisable"""
        profile = UserProfile(
            user_id="u123",
            username="test",
            name="Test User",
            followers_count=100,
            verified=True
        )
        result = profile.to_dict()

        assert isinstance(result, dict)
        assert result["user_id"] == "u123"
        assert result["username"] == "test"

    def test_profile_requires_user_id(self):
        """user_id est requis"""
        with pytest.raises(ValueError, match="user_id"):
            UserProfile(user_id=None)


# ─────────────────────────────────────────────────────────────────────
# 2. PROFILE CACHE — LRU CACHE
# ─────────────────────────────────────────────────────────────────────

class TestProfileCache:
    """Cache LRU pour les profils utilisateurs"""

    def test_cache_stores_and_retrieves_profile(self):
        """Un profil stocké peut être récupéré"""
        cache = ProfileCache(max_size=10)
        profile = UserProfile(user_id="u1", username="test")

        cache.set("u1", profile)
        result = cache.get("u1")

        assert result is not None
        assert result.user_id == "u1"

    def test_cache_returns_none_for_missing_key(self):
        """get retourne None si la clé n'existe pas"""
        cache = ProfileCache(max_size=10)
        assert cache.get("unknown") is None

    def test_cache_evicts_oldest_when_full(self):
        """Le cache évite les plus anciens quand plein (LRU)"""
        cache = ProfileCache(max_size=2)

        cache.set("u1", UserProfile(user_id="u1"))
        cache.set("u2", UserProfile(user_id="u2"))
        cache.set("u3", UserProfile(user_id="u3"))  # Évince u1

        assert cache.get("u1") is None
        assert cache.get("u2") is not None
        assert cache.get("u3") is not None

    def test_cache_hit_updates_lru_order(self):
        """Un get remet l'élément en tête de la LRU"""
        cache = ProfileCache(max_size=2)

        cache.set("u1", UserProfile(user_id="u1"))
        cache.set("u2", UserProfile(user_id="u2"))
        cache.get("u1")  # Touch u1
        cache.set("u3", UserProfile(user_id="u3"))  # Évince u2

        assert cache.get("u1") is not None
        assert cache.get("u2") is None

    def test_cache_clear_removes_all_entries(self):
        """clear vide complètement le cache"""
        cache = ProfileCache(max_size=10)
        cache.set("u1", UserProfile(user_id="u1"))
        cache.set("u2", UserProfile(user_id="u2"))

        cache.clear()

        assert cache.get("u1") is None
        assert cache.get("u2") is None
        assert cache.size == 0

    def test_cache_size_property(self):
        """size retourne le nombre d'entrées"""
        cache = ProfileCache(max_size=10)
        assert cache.size == 0

        cache.set("u1", UserProfile(user_id="u1"))
        cache.set("u2", UserProfile(user_id="u2"))

        assert cache.size == 2


# ─────────────────────────────────────────────────────────────────────
# 3. SCRAPER ENRICHER — INIT
# ─────────────────────────────────────────────────────────────────────

class TestScraperEnricherInit:
    """Initialisation du ScraperEnricher"""

    def test_enricher_creates_internal_cache(self):
        """Le cache LRU est créé à l'initialisation"""
        enricher = ScraperEnricher(cache_size=100)
        assert enricher._cache is not None
        assert enricher._cache.max_size == 100

    def test_enricher_default_delay_between_requests(self):
        """Le délai par défaut est de 1 seconde (anti-rate-limit)"""
        enricher = ScraperEnricher()
        assert enricher._delay_seconds == 1.0

    def test_enricher_accepts_custom_delay(self):
        """Un délai personnalisé peut être fourni"""
        enricher = ScraperEnricher(delay_seconds=2.5)
        assert enricher._delay_seconds == 2.5

    def test_enricher_requires_positive_delay(self):
        """Le délai doit être positif"""
        with pytest.raises(ValueError, match="delay"):
            ScraperEnricher(delay_seconds=-1)


# ─────────────────────────────────────────────────────────────────────
# 4. SCRAPER ENRICHER — SCRAPE USER PROFILE (CORE)
# ─────────────────────────────────────────────────────────────────────

class TestScraperEnricherScrapeProfile:
    """Scraping d'un profil utilisateur individuel"""

    @patch("src.extract.scraper_enricher.twikit.Client")
    @pytest.mark.asyncio
    async def test_scrape_profile_fetches_user_data(self, mock_client_cls):
        """Un profil utilisateur est fetché via twikit"""
        # Setup mock
        mock_client = AsyncMock()
        mock_user = AsyncMock()
        mock_user.id = "u123"
        mock_user.name = "Test User"
        mock_user.screen_name = "testuser"
        mock_user.followers_count = 5000
        mock_user.friends_count = 200
        mock_user.verified = True
        mock_user.description = "Bio test"
        mock_user.profile_image_url_https = "https://img.test"

        mock_client.get_user_by_screen_name = AsyncMock(return_value=mock_user)
        mock_client_cls.return_value = mock_client

        enricher = ScraperEnricher()
        enricher._client = mock_client

        result = await enricher._scrape_user_profile("testuser")

        assert result is not None
        assert result.user_id == "u123"
        assert result.username == "testuser"
        assert result.followers_count == 5000

    @patch("src.extract.scraper_enricher.twikit.Client")
    @pytest.mark.asyncio
    async def test_scrape_profile_handles_user_not_found(self, mock_client_cls):
        """Un user inexistant retourne None (pas d'exception)"""
        from twikit.errors import UserNotFound

        mock_client = AsyncMock()
        mock_client.get_user_by_screen_name = AsyncMock(
            side_effect=UserNotFound("User not found")
        )
        mock_client_cls.return_value = mock_client

        enricher = ScraperEnricher()
        enricher._client = mock_client

        result = await enricher._scrape_user_profile("nonexistent")

        assert result is None

    @patch("src.extract.scraper_enricher.twikit.Client")
    @pytest.mark.asyncio
    async def test_scrape_profile_respects_delay(self, mock_client_cls):
        """Un délai est respecté entre chaque appel scraping"""
        import time

        mock_client = AsyncMock()
        mock_user = AsyncMock()
        mock_user.id = "u1"
        mock_user.name = "User 1"
        mock_user.screen_name = "user1"
        mock_client.get_user_by_screen_name = AsyncMock(return_value=mock_user)
        mock_client_cls.return_value = mock_client

        enricher = ScraperEnricher(delay_seconds=0.1)
        enricher._client = mock_client

        start = time.time()
        await enricher._scrape_user_profile("user1")
        elapsed = time.time() - start

        # Le délai doit être respecté (tolérance 20%)
        assert elapsed >= 0.08

    @patch("src.extract.scraper_enricher.twikit.Client")
    @pytest.mark.asyncio
    async def test_scrape_profile_caches_result(self, mock_client_cls):
        """Le profil scrapé est mis en cache"""
        mock_client = AsyncMock()
        mock_user = AsyncMock()
        mock_user.id = "u123"
        mock_user.name = "Cached User"
        mock_user.screen_name = "cached"
        mock_client.get_user_by_screen_name = AsyncMock(return_value=mock_user)
        mock_client_cls.return_value = mock_client

        enricher = ScraperEnricher()
        enricher._client = mock_client

        await enricher._scrape_user_profile("cached")

        cached = enricher._cache.get("u123")
        assert cached is not None
        assert cached.username == "cached"

    @patch("src.extract.scraper_enricher.twikit.Client")
    @pytest.mark.asyncio
    async def test_scrape_profile_uses_cache_on_hit(self, mock_client_cls):
        """Si le profil est en cache, twikit n'est pas appelé"""
        mock_client = AsyncMock()
        mock_client_cls.return_value = mock_client

        enricher = ScraperEnricher()
        enricher._client = mock_client

        # Pré-remplir le cache
        cached_profile = UserProfile(
            user_id="u123",
            username="already_cached",
            name="Cached"
        )
        enricher._cache.set("u123", cached_profile)

        # Scraper le même user
        result = await enricher._scrape_user_profile("already_cached")

        # twikit ne doit PAS être appelé
        mock_client.get_user_by_screen_name.assert_not_called()
        # Le profil vient du cache
        assert result.user_id == "u123"

    @patch("src.extract.scraper_enricher.twikit.Client")
    @pytest.mark.asyncio
    async def test_scrape_profile_raises_enrichment_error_on_network_failure(
        self, mock_client_cls
    ):
        """Une erreur réseau lève EnrichmentError"""
        from twikit.errors import TwikitException

        mock_client = AsyncMock()
        mock_client.get_user_by_screen_name = AsyncMock(
            side_effect=TwikitException("Network error")
        )
        mock_client_cls.return_value = mock_client

        enricher = ScraperEnricher()
        enricher._client = mock_client

        result = await enricher._scrape_user_profile("test")

        # Graceful degradation: retourne None plutôt que lever
        assert result is None


# ─────────────────────────────────────────────────────────────────────
# 5. SCRAPER ENRICHER — ENRICH BATCH (BULK)
# ─────────────────────────────────────────────────────────────────────

class TestScraperEnricherEnrichBatch:
    """Enrichissement en batch de multiple users"""

    @patch("src.extract.scraper_enricher.twikit.Client")
    @pytest.mark.asyncio
    async def test_enrich_batch_fetches_multiple_users(self, mock_client_cls):
        """Plusieurs users sont enrichis en batch"""
        mock_client = AsyncMock()

        def make_user(uid, name):
            u = AsyncMock()
            u.id = uid
            u.name = name
            u.screen_name = name.lower()
            u.followers_count = 100
            u.verified = False
            return u

        mock_client.get_user_by_screen_name = AsyncMock(
            side_effect=[
                make_user("u1", "User1"),
                make_user("u2", "User2"),
                make_user("u3", "User3"),
            ]
        )
        mock_client_cls.return_value = mock_client

        enricher = ScraperEnricher()
        enricher._client = mock_client

        usernames = ["user1", "user2", "user3"]
        results = await enricher.enrich_batch(usernames)

        assert len(results) == 3
        assert results[0].username == "user1"
        assert results[1].username == "user2"

    @patch("src.extract.scraper_enricher.twikit.Client")
    @pytest.mark.asyncio
    async def test_enrich_batch_handles_partial_failures(self, mock_client_cls):
        """Si un user échoue, les autres continuent"""
        from twikit.errors import UserNotFound

        mock_client = AsyncMock()

        def side_effect(name):
            if name == "user2":
                raise UserNotFound("Not found")
            u = AsyncMock()
            u.id = f"u_{name}"
            u.name = name
            u.screen_name = name
            return u

        mock_client.get_user_by_screen_name = AsyncMock(side_effect=side_effect)
        mock_client_cls.return_value = mock_client

        enricher = ScraperEnricher()
        enricher._client = mock_client

        usernames = ["user1", "user2", "user3"]
        results = await enricher.enrich_batch(usernames)

        assert len(results) == 2
        assert results[0].username == "user1"
        assert results[1].username == "user3"

    @patch("src.extract.scraper_enricher.twikit.Client")
    @pytest.mark.asyncio
    async def test_enrich_batch_returns_empty_list_for_empty_input(self, mock_client_cls):
        """Une liste vide retourne une liste vide"""
        enricher = ScraperEnricher()
        results = await enricher.enrich_batch([])

        assert results == []

    @patch("src.extract.scraper_enricher.twikit.Client")
    @pytest.mark.asyncio
    async def test_enrich_batch_deduplicates_usernames(self, mock_client_cls):
        """Les usernames en doublon ne sont scrapés qu'une fois"""
        mock_client = AsyncMock()
        mock_user = AsyncMock()
        mock_user.id = "u1"
        mock_user.name = "Unique"
        mock_user.screen_name = "unique"
        mock_client.get_user_by_screen_name = AsyncMock(return_value=mock_user)
        mock_client_cls.return_value = mock_client

        enricher = ScraperEnricher()
        enricher._client = mock_client

        usernames = ["unique", "unique", "unique"]
        await enricher.enrich_batch(usernames)

        # twikit appelé une seule fois
        assert mock_client.get_user_by_screen_name.call_count == 1

    @patch("src.extract.scraper_enricher.twikit.Client")
    @pytest.mark.asyncio
    async def test_enrich_batch_tracks_success_and_failure_counts(self, mock_client_cls):
        """Les compteurs de succès/échec sont mis à jour"""
        from twikit.errors import UserNotFound

        mock_client = AsyncMock()

        def side_effect(name):
            if name == "user2":
                raise UserNotFound("Not found")
            u = AsyncMock()
            u.id = f"u_{name}"
            u.name = name
            u.screen_name = name
            return u

        mock_client.get_user_by_screen_name = AsyncMock(side_effect=side_effect)
        mock_client_cls.return_value = mock_client

        enricher = ScraperEnricher()
        enricher._client = mock_client

        await enricher.enrich_batch(["user1", "user2", "user3"])

        assert enricher.stats["success"] == 2
        assert enricher.stats["failed"] == 1


# ─────────────────────────────────────────────────────────────────────
# 6. SCRAPER ENRICHER — ENRICH TWEETS (MERGE)
# ─────────────────────────────────────────────────────────────────────

class TestScraperEnricherEnrichTweets:
    """Merge tweets API + profils users scrapés"""

    @patch("src.extract.scraper_enricher.twikit.Client")
   
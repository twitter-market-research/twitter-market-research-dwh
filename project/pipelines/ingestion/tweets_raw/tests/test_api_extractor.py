# tests/test_api_extractor.py
import pytest
from unittest.mock import patch, MagicMock

from project.pipelines.ingestion.tweets_raw.utils.api_extractor import (
    APIExtractor, SearchConfig, APIError
)


def _make_mock_response(status_code, json_data):
    """Construit une réponse HTTP mockée (façon requests.Response).

    Parameters
    ----------
    status_code : int
        Code HTTP simulé (ex: 200).
    json_data : dict
        Charge utile renvoyée par ``response.json()``.

    Returns
    -------
    MagicMock
        Mock exposant ``status_code`` et ``json()``.
    """
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_data
    return response


# ─────────────────────────────────────────────────────────────────────
# 1. CONFIGURATION — SEARCH CONFIG
# ─────────────────────────────────────────────────────────────────────

class TestSearchConfig:
    """Configuration des paramètres de recherche"""

    def test_config_default_values_for_ligue1(self):
        """Par défaut : mots-clés Ligue1, max 250, sans filtre date"""
        config = SearchConfig()
        assert "Ligue1" in config.query
        assert config.max_results == 250
        assert config.start_time is None

    def test_config_enforces_max_results_250_cap(self):
        """max_results est hard-capé à 250 par collecte"""
        config = SearchConfig(max_results=1000)
        assert config.max_results == 250

    def test_config_rejects_max_results_below_10(self):
        """max_results minimum est 10 (cohérence API)"""
        with pytest.raises(ValueError, match="max_results"):
            SearchConfig(max_results=5)

    def test_config_builds_query_with_operators(self):
        """La requête utilise les opérateurs X API v2"""
        config = SearchConfig(
            keywords="Ligue1 OR PSG OR OM",
            lang="fr",
            min_retweets=10
        )
        assert "Ligue1" in config.query
        assert "min_retweets:10" in config.query

    def test_config_adds_expansions_for_api_response(self):
        """Les expansions demandées sont stockées"""
        config = SearchConfig(
            expansions=["author_id", "attachments.media_keys"],
            tweet_fields=["created_at", "public_metrics", "lang"]
        )
        assert "author_id" in config.expansions
        assert "created_at" in config.tweet_fields

    def test_config_default_expansions_include_author_id(self):
        """Par défaut, author_id est demandé pour l'enrichissement ultérieur"""
        config = SearchConfig()
        assert "author_id" in config.expansions


# ─────────────────────────────────────────────────────────────────────
# 2. API EXTRACTOR — INIT
# ─────────────────────────────────────────────────────────────────────

class TestAPIExtractorInit:
    """Initialisation de l'APIExtractor"""

    def test_extractor_rejects_empty_bearer_token(self):
        """Un token vide lève une erreur"""
        with pytest.raises(ValueError, match="Bearer token"):
            APIExtractor(bearer_token="")

    def test_extractor_rejects_none_bearer_token(self):
        """Un token None lève une erreur"""
        with pytest.raises(ValueError, match="Bearer token"):
            APIExtractor(bearer_token=None)

    def test_extractor_accepts_valid_bearer_token(self):
        """Un token valide initialise l'extracteur"""
        extractor = APIExtractor(
            bearer_token="AAAAAAAAAAAAAAAAAAAAAFAKE_TOKEN",
            config=SearchConfig()
        )
        assert extractor is not None
        assert extractor._config.max_results == 250

    def test_extractor_sets_default_base_url(self):
        """L'URL de base pointe vers api.x.com par défaut"""
        extractor = APIExtractor(
            bearer_token="FAKE",
            config=SearchConfig()
        )
        assert "api.x.com" in extractor._base_url


# ─────────────────────────────────────────────────────────────────────
# 3. API EXTRACTOR — SEARCH TWEETS (CORE)
# ─────────────────────────────────────────────────────────────────────

class TestAPIExtractorSearchTweets:
    """Recherche de tweets via l'API X — fonction core"""

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_search_tweets_hits_correct_endpoint(self, mock_get):
        """L'endpoint /2/tweets/search/recent est appelé"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": []}
        mock_get.return_value = mock_response

        extractor = APIExtractor(
            bearer_token="FAKE",
            config=SearchConfig(keywords="Ligue1")
        )
        extractor.search_tweets()

        mock_get.assert_called_once()
        url = mock_get.call_args[0][0]
        assert "api.x.com/2/tweets/search/recent" in url

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_search_tweets_sends_authorization_header(self, mock_get):
        """Le header Authorization: Bearer <token> est envoyé"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": []}
        mock_get.return_value = mock_response

        extractor = APIExtractor(
            bearer_token="AAAAAAAAAAAAAAAAAAAAAFAKE",
            config=SearchConfig()
        )
        extractor.search_tweets()

        headers = mock_get.call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer AAAAAAAAAAAAAAAAAAAAAFAKE"

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_search_tweets_passes_query_params(self, mock_get):
        """Les params sont : query, max_results, expansions, tweet.fields"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": []}
        mock_get.return_value = mock_response

        extractor = APIExtractor(
            bearer_token="FAKE",
            config=SearchConfig(
                keywords="Ligue1",
                max_results=250,
                lang="fr",
                start_time="2026-03-28T00:00:00Z"
            )
        )
        extractor.search_tweets()

        params = mock_get.call_args[1]["params"]
        assert "Ligue1" in params["query"]
        # L'API X v2 plafonne max_results à 100 par requête.
        assert params["max_results"] == 100
        # Le filtre de langue est injecté dans la query (lang:fr), pas en param.
        assert "lang:fr" in params["query"]
        assert params["start_time"] == "2026-03-28T00:00:00Z"

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_search_tweets_passes_expansions_and_fields(self, mock_get):
        """Les expansions et tweet.fields sont passés en params"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": []}
        mock_get.return_value = mock_response

        extractor = APIExtractor(
            bearer_token="FAKE",
            config=SearchConfig(
                expansions=["author_id"],
                tweet_fields=["created_at", "public_metrics", "lang"]
            )
        )
        extractor.search_tweets()

        params = mock_get.call_args[1]["params"]
        assert params["expansions"] == "author_id"
        assert "created_at" in params["tweet.fields"]
        assert "public_metrics" in params["tweet.fields"]

    def test_default_config_requests_entities_for_hashtags(self):
        """Le schéma par défaut doit demander `entities`.

        Sans ce champ, l'API X ne renvoie pas `entities.hashtags`, et donc le
        Serializer produit des hashtags vides et le KeyBuilder tombe toujours
        sur la clé de repli (bug B).
        """
        config = SearchConfig()

        assert "entities" in config.tweet_fields

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_search_tweets_returns_list_of_tweets(self, mock_get):
        """Retourne une liste de dicts (tweets bruts)"""
        expected_data = [
            {"id": "1", "text": "Tweet 1", "author_id": "u1"},
            {"id": "2", "text": "Tweet 2", "author_id": "u2"}
        ]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": expected_data}
        mock_get.return_value = mock_response

        extractor = APIExtractor(bearer_token="FAKE", config=SearchConfig())
        result = extractor.search_tweets()

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["id"] == "1"

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_search_tweets_returns_empty_list_when_no_data(self, mock_get):
        """Réponse sans clé 'data' retourne liste vide"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_get.return_value = mock_response

        extractor = APIExtractor(bearer_token="FAKE", config=SearchConfig())
        result = extractor.search_tweets()

        assert result == []

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_search_tweets_handles_http_error_gracefully(self, mock_get):
        """Une erreur HTTP retourne liste vide (pas d'exception non gérée)"""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        extractor = APIExtractor(bearer_token="FAKE", config=SearchConfig())
        result = extractor.search_tweets()

        assert result == []

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_search_tweets_raises_on_401_unauthorized(self, mock_get):
        """Erreur 401 lève une APIError spécifique"""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"errors": [{"title": "Unauthorized"}]}
        mock_get.return_value = mock_response

        extractor = APIExtractor(bearer_token="INVALID", config=SearchConfig())

        with pytest.raises(APIError, match="401"):
            extractor.search_tweets()

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_search_tweets_raises_on_403_forbidden(self, mock_get):
        """Erreur 403 lève une APIError spécifique"""
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_get.return_value = mock_response

        extractor = APIExtractor(bearer_token="FAKE", config=SearchConfig())

        with pytest.raises(APIError, match="403"):
            extractor.search_tweets()

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_search_tweets_logs_on_429_rate_limit(self, mock_get, caplog):
        """Erreur 429 est loggée mais ne lève pas d'exception"""
        import logging
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.json.return_value = {"errors": [{"title": "Rate limit"}]}
        mock_get.return_value = mock_response

        extractor = APIExtractor(bearer_token="FAKE", config=SearchConfig())

        with caplog.at_level(logging.WARNING):
            result = extractor.search_tweets()

        assert result == []
        assert any("429" in msg for msg in caplog.messages)

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_search_tweets_extracts_author_ids_from_response(self, mock_get):
        """Les author_id sont extraits des tweets retournés"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [
                {"id": "1", "author_id": "u1", "text": "T1"},
                {"id": "2", "author_id": "u2", "text": "T2"},
                {"id": "3", "author_id": "u1", "text": "T3"}  # doublon author
            ]
        }
        mock_get.return_value = mock_response

        extractor = APIExtractor(bearer_token="FAKE", config=SearchConfig())
        result = extractor.search_tweets()

        assert len(result) == 3
        author_ids = {t["author_id"] for t in result}
        assert author_ids == {"u1", "u2"}


# ─────────────────────────────────────────────────────────────────────
# 4. API EXTRACTOR — PAGINATION
# ─────────────────────────────────────────────────────────────────────

class TestAPIExtractorPagination:
    """Gestion de la pagination (next_token)"""

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_search_tweets_handles_next_token_pagination(self, mock_get):
        """Si next_token est présent, une 2e requête est faite"""
        first_response = {
            "data": [{"id": "1", "text": "T1", "author_id": "u1"}],
            "meta": {"next_token": "ABC123", "result_count": 1}
        }
        second_response = {
            "data": [{"id": "2", "text": "T2", "author_id": "u2"}],
            "meta": {"result_count": 1}
        }

        mock_get.side_effect = [
            _make_mock_response(200, first_response),
            _make_mock_response(200, second_response)
        ]

        extractor = APIExtractor(bearer_token="FAKE", config=SearchConfig())
        result = extractor.search_tweets()

        assert mock_get.call_count == 2
        assert len(result) == 2
        # La 2e requête doit inclure next_token
        second_call_params = mock_get.call_args_list[1][1]["params"]
        assert second_call_params["pagination_token"] == "ABC123"

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_search_tweets_limits_pagination_to_avoid_budget(self, mock_get):
        """La pagination s'arrête à max_results (250) pour protéger le budget"""
        def mock_responses(*args, **kwargs):
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "data": [{"id": str(i), "text": f"T{i}", "author_id": "u"}
                         for i in range(100)],
                "meta": {"next_token": "TOKEN", "result_count": 100}
            }
            return response

        mock_get.side_effect = [mock_responses() for _ in range(5)]

        extractor = APIExtractor(
            bearer_token="FAKE",
            config=SearchConfig(max_results=250)
        )
        result = extractor.search_tweets()

        # 250 max = 2 pages de 100 + 1 page de 50
        # La 3e page ne devrait pas être appelée (250 atteint)
        assert mock_get.call_count <= 3
        assert len(result) <= 250


# ─────────────────────────────────────────────────────────────────────
# 5. API EXTRACTOR — BUDGET TRACKING
# ─────────────────────────────────────────────────────────────────────

class TestAPIExtractorBudgetTracking:
    """Tracking des coûts API pour respecter le budget de 25$"""

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_extractor_tracks_tweets_fetched_count(self, mock_get):
        """Le nombre de tweets fetchés est tracké"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{"id": str(i), "text": f"T{i}", "author_id": "u"}
                     for i in range(50)]
        }
        mock_get.return_value = mock_response

        extractor = APIExtractor(bearer_token="FAKE", config=SearchConfig())
        extractor.search_tweets()

        assert extractor.tweets_fetched == 50

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_extractor_estimates_cost(self, mock_get):
        """Le coût estimé est calculé (0.005$ par tweet)"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{"id": str(i), "text": f"T{i}", "author_id": "u"}
                     for i in range(100)]
        }
        mock_get.return_value = mock_response

        extractor = APIExtractor(bearer_token="FAKE", config=SearchConfig())
        extractor.search_tweets()

        assert extractor.estimated_cost == pytest.approx(0.50, rel=0.01)

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_extractor_warns_when_approaching_budget(self, mock_get, caplog):
        """Un warning est émis quand on approche 80% du budget"""
        import logging
        # Simuler qu'on a déjà fetché 4000 tweets (80% de 5000)
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{"id": str(i), "text": f"T{i}", "author_id": "u"}
                     for i in range(100)]
        }
        mock_get.return_value = mock_response

        extractor = APIExtractor(bearer_token="FAKE", config=SearchConfig())
        extractor._tweets_fetched = 4000  # Injecté manuellement
        extractor.search_tweets()

        assert any("budget" in msg.lower() for msg in caplog.messages)

    @patch("project.pipelines.ingestion.tweets_raw.utils.api_extractor.requests.get")
    def test_extractor_blocks_when_budget_exceeded(self, mock_get):
        """La recherche est bloquée si le budget de 25$ est dépassé"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "data": [{"id": str(i), "text": f"T{i}", "author_id": "u"}
                     for i in range(100)]
        }
        mock_get.return_value = mock_response

        extractor = APIExtractor(bearer_token="FAKE", config=SearchConfig())
        extractor._tweets_fetched = 5000  # Injecté manuellement (25$ atteint)

        with pytest.raises(APIError, match="budget"):
            extractor.search_tweets()
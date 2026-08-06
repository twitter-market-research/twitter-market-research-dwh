"""
APIExtractor — Couche d'extraction via l'API X (Twitter) v2.

Stratégie hybride :
  - API X (pay-per-use) → collecte les tweets bruts
  - Coût estimé : 0.005$ par tweet lu
  - Budget max : 25$ = ~5000 tweets
  - Limite par collecte : 250 tweets max

Architecture :
  Extractor (hybride)
      └── APIExtractor (ce module) → tweets bruts avec author_id
      └── ScraperEnricher (à venir) → profils utilisateurs via twikit
"""

import logging
import requests
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────────────────────────────

class APIError(Exception):
    """Erreur spécifique à l'API X"""
    def __init__(self, message: str, status_code: Optional[int] = None):
        self.message = message
        self.status_code = status_code
        super().__init__(f"APIError {status_code}: {message}")


class BudgetExceededError(APIError):
    """Le budget API a été dépassé"""
    pass


# ─────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────

@dataclass
class SearchConfig:
    """
    Configuration d'une recherche de tweets via l'API X v2.

    Attributes
    ----------
    keywords : str
        Mots-clés de recherche (Ligue1, PSG, OM, etc.)
    max_results : int
        Nombre max de tweets à collecter par collecte. Hard-cap à 250.
    lang : Optional[str]
        Filtre de langue (ex: "fr"). Par défaut None (toutes langues).
    start_time : Optional[str]
        Filtre de date (format ISO 8601). Par défaut None (pas de filtre).
    end_time : Optional[str]
        Filtre de date de fin (format ISO 8601).
    expansions : List[str]
        Champs à expandre (author_id, attachments.media_keys, etc.).
    tweet_fields : List[str]
        Champs de tweet à récupérer.
    user_fields : List[str]
        Champs d'utilisateur à récupérer (si author_id dans expansions).
    min_retweets : Optional[int]
        Filtre minimum de retweets (ajoute "min_retweets:N" à la query).
    query : Optional[str]
        Requête manuelle (surcharge keywords). Si fourni, keywords est ignoré.
    """
    keywords: str = "Ligue1"
    max_results: int = 250
    lang: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    expansions: List[str] = field(default_factory=lambda: ["author_id"])
    tweet_fields: List[str] = field(default_factory=lambda: [
        "created_at", "public_metrics", "lang", "text", "id",
        "author_id", "context_annotations", "entities",
    ])
    user_fields: List[str] = field(default_factory=lambda: [
        "username", "name", "verified", "public_metrics"
    ])
    min_retweets: Optional[int] = None
    query: Optional[str] = None

    def __post_init__(self):
        # Hard-cap à 250 pour protéger le budget
        MAX_RESULTS_PER_COLLECT = 250
        MIN_RESULTS_PER_COLLECT = 10

        if self.max_results > MAX_RESULTS_PER_COLLECT:
            logger.warning(
                f"max_results={self.max_results} dépassé, "
                f"capé à {MAX_RESULTS_PER_COLLECT} (protection budget)"
            )
            self.max_results = MAX_RESULTS_PER_COLLECT

        if self.max_results < MIN_RESULTS_PER_COLLECT:
            raise ValueError(
                f"max_results doit être >= {MIN_RESULTS_PER_COLLECT}, "
                f"reçu {self.max_results}"
            )

        # Si query manuelle fournie, on l'utilise telle quelle
        if self.query is None:
            self.query = self._build_query()

    def _build_query(self) -> str:
        """Construit la requête X API v2 à partir des paramètres."""
        parts = []

        # Mots-clés de base
        base_keywords = self.keywords
        parts.append(f"({base_keywords})")

        # Filtre minimum de retweets
        if self.min_retweets is not None:
            parts.append(f"min_retweets:{self.min_retweets}")

        # Optionnel : exclure les retweets (garder tweets originaux)
        parts.append("-is:retweet")

        query = " ".join(parts)
        logger.debug(f"Requête API X construite: {query}")
        return query


# ─────────────────────────────────────────────────────────────────────
# EXTRACTOR
# ─────────────────────────────────────────────────────────────────────

class APIExtractor:
    """
    Extracteur de tweets via l'API X v2 (endpoint /2/tweets/search/recent).

    Usage
    -----
    >>> config = SearchConfig(keywords="PSG OR OM", lang="fr", max_results=250)
    >>> extractor = APIExtractor(bearer_token="...", config=config)
    >>> tweets = extractor.search_tweets()
    >>> print(f"{len(tweets)} tweets, coût: {extractor.estimated_cost:.2f}$")

    Attributes
    ----------
    bearer_token : str
        Bearer token d'authentification API X.
    _config : SearchConfig
        Configuration de recherche.
    _base_url : str
        URL de base de l'API X.
    _tweets_fetched : int
        Compteur cumulé de tweets fetchés (pour le budget).
    _budget_limit : float
        Budget max en dollars (défaut: 25$).
    _cost_per_tweet : float
        Coût unitaire par tweet lu (défaut: 0.005$).
    """

    BASE_URL = "https://api.x.com/2/tweets/search/recent"
    COST_PER_TWEET = 0.005  # Pay-per-use X API (février 2026)

    def __init__(
        self,
        bearer_token: str,
        config: Optional[SearchConfig] = None,
        budget_limit: float = 25.0
    ):
        if not bearer_token or not bearer_token.strip():
            raise ValueError(
                "Bearer token requis. Fournissez votre token API X v2."
            )

        self.bearer_token = bearer_token.strip()
        self._config = config or SearchConfig()
        self._base_url = self.BASE_URL
        self._tweets_fetched = 0
        self._budget_limit = budget_limit
        self._cost_per_tweet = self.COST_PER_TWEET

        logger.info(
            f"APIExtractor initialisé — Budget: {budget_limit}$, "
            f"Coût/tweet: {self._cost_per_tweet}$, "
            f"Limit par collecte: {self._config.max_results} tweets"
        )

    @property
    def tweets_fetched(self) -> int:
        """Nombre total de tweets fetchés (tracking budget)."""
        return self._tweets_fetched

    @property
    def estimated_cost(self) -> float:
        """Coût estimé cumulé en dollars."""
        return self._tweets_fetched * self._cost_per_tweet

    @property
    def remaining_budget(self) -> float:
        """Budget restant en dollars."""
        return max(0, self._budget_limit - self.estimated_cost)

    @property
    def remaining_tweets(self) -> int:
        """Nombre de tweets restants avant d'atteindre le budget."""
        return int(self.remaining_budget / self._cost_per_tweet)

    def _check_budget(self) -> None:
        """Vérifie que le budget n'est pas dépassé avant une collecte."""
        if self.estimated_cost >= self._budget_limit:
            raise BudgetExceededError(
                f"Budget API dépassé: {self.estimated_cost:.2f}$ >= "
                f"{self._budget_limit}$ (limite). "
                f"Arrêtez de collecter ou augmentez le budget."
            )

        # Warning à 80% du budget
        if self.estimated_cost >= self._budget_limit * 0.80:
            percent_used = (
                self.estimated_cost / self._budget_limit * 100
            )
            logger.warning(
                f"BUDGET ATTENTION: {self.estimated_cost:.2f}$ / "
                f"{self._budget_limit}$ consommés ({percent_used:.0f}%). "
                f"Reste ~{self.remaining_tweets} tweets."
            )

    def _make_request(
        self,
        params: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Effectue une requête GET vers l'API X.

        Returns
        -------
        Optional[Dict]
            La réponse JSON de l'API, ou None en cas d'erreur non bloquante.

        Raises
        ------
        APIError
            Pour les erreurs 401 (Unauthorized) et 403 (Forbidden).
        """
        headers = {
            "Authorization": f"Bearer {self.bearer_token}",
            "User-Agent": "TwitterMarketResearch/1.0"
        }

        logger.debug(f"Requête API: {self._base_url} params={params}")

        try:
            response = requests.get(
                self._base_url,
                headers=headers,
                params=params,
                timeout=30
            )
        except requests.RequestException as e:
            logger.error(f"Erreur réseau API X: {e}")
            return None

        logger.debug(f"Réponse API: status={response.status_code}")

        # Gestion des codes HTTP
        if response.status_code == 200:
            return response.json()

        # Erreurs bloquantes
        if response.status_code == 401:
            is_json = response.headers.get("content-type", "").startswith(
                "application/json"
            )
            error_data = response.json() if is_json else {}
            error_msg = error_data.get("errors", [{}])[0].get(
                "title", "Unauthorized"
            )
            raise APIError(f"Token invalide ou expiré: {error_msg}", 401)

        if response.status_code == 403:
            raise APIError(
                "Accès interdit — vérifiez vos permissions API", 403
            )

        if response.status_code == 404:
            logger.error("Endpoint API non trouvé — l'URL a peut-être changé")
            return None

        # Rate limit — log mais on ne lève pas d'exception
        if response.status_code == 429:
            is_json = response.headers.get("content-type", "").startswith(
                "application/json"
            )
            error_data = response.json() if is_json else {}
            error_msg = error_data.get("errors", [{}])[0].get(
                "title", "Rate limit exceeded"
            )
            logger.warning(
                f"Rate limit API X (429): {error_msg}. "
                f"Attendez avant de relancer."
            )
            return None

        # Autres erreurs
        logger.error(
            f"Erreur API X (status={response.status_code}): "
            f"{response.text[:200]}"
        )
        return None

    def search_tweets(self) -> List[Dict[str, Any]]:
        """
        Recherche des tweets via l'API X v2 avec pagination.

        Returns
        -------
        List[Dict[str, Any]]
            Liste des tweets bruts (dictionnaires) retournés par l'API.
            Retourne une liste vide en cas d'erreur non bloquante.

        Raises
        ------
        BudgetExceededError
            Si le budget est dépassé avant la collecte.
        APIError
            Pour les erreurs d'authentification (401, 403).
        """
        # Vérification budget avant de commencer
        self._check_budget()

        # Construire les params de la requête initiale
        params = self._build_request_params()

        all_tweets: List[Dict[str, Any]] = []
        next_token: Optional[str] = None
        page = 0

        logger.info(
            f"Démarrage collecte — Requête: '{params['query']}', "
            f"max_results: {self._config.max_results}"
        )

        while True:
            page += 1

            # Vérifier qu'on n'a pas déjà atteint max_results
            if len(all_tweets) >= self._config.max_results:
                logger.debug(
                    f"Max résultats atteint ({len(all_tweets)} tweets), "
                    f"arrêt pagination"
                )
                break

            # Adapter max_results pour la dernière page
            remaining = self._config.max_results - len(all_tweets)
            # API max per request = 100
            params["max_results"] = max(10, min(remaining, 100))

            # Ajouter pagination token si présent
            if next_token:
                params["pagination_token"] = next_token

            # Faire la requête
            data = self._make_request(params)

            if data is None:
                logger.warning(
                    f"Aucune donnée retournée par l'API (page {page})"
                )
                break

            # Extraire les tweets
            tweets = data.get("data", [])
            if not tweets:
                logger.debug("Aucun tweet trouvé pour cette requête")
                break

            all_tweets.extend(tweets)
            logger.debug(
                f"Page {page}: {len(tweets)} tweets collectés "
                f"(total: {len(all_tweets)})"
            )

            # Vérifier la pagination
            meta = data.get("meta", {})
            next_token = meta.get("next_token")

            if not next_token:
                logger.debug("Dernière page atteinte (pas de next_token)")
                break

        # Filet de sécurité budget : ne jamais dépasser max_results, même si
        # l'API renvoie plus que demandé sur la dernière page.
        if len(all_tweets) > self._config.max_results:
            all_tweets = all_tweets[:self._config.max_results]

        # Mettre à jour le compteur budget
        tweets_count = len(all_tweets)
        self._tweets_fetched += tweets_count

        logger.info(
            f"Collecte terminée — {tweets_count} tweets, "
            f"coût estimé: {self.estimated_cost:.2f}$, "
            f"budget restant: {self.remaining_budget:.2f}$"
        )

        return all_tweets

    def _build_request_params(self) -> Dict[str, Any]:
        """
        Construit le dictionnaire de paramètres pour la requête API.

        Returns
        -------
        Dict[str, Any]
            Paramètres à passer à requests.get().
        """
        params: Dict[str, Any] = {
            "query": self._config.query,
            "max_results": min(self._config.max_results, 100),  # API hard limit
            "expansions": ",".join(self._config.expansions),
            "tweet.fields": ",".join(self._config.tweet_fields),
        }

        # Filtre de langue
        if self._config.lang:
            params["query"] = f"{params['query']} lang:{self._config.lang}"
            logger.debug(f"Filtre langue ajouté à la query: lang:{self._config.lang}")

        # Filtres temporels
        if self._config.start_time:
            params["start_time"] = self._config.start_time

        if self._config.end_time:
            params["end_time"] = self._config.end_time

        # User fields (si author_id dans expansions)
        if "author_id" in self._config.expansions:
            params["user.fields"] = ",".join(self._config.user_fields)

        logger.debug(f"Params API construits: {params}")
        return params

    def get_author_ids(self, tweets: List[Dict[str, Any]]) -> List[str]:
        """
        Extrait les author_id uniques d'une liste de tweets.

        Utile pour le passage à l'étape d'enrichissement via twikit.

        Parameters
        ----------
        tweets : List[Dict[str, Any]]
            Liste de tweets bruts retournés par search_tweets().

        Returns
        -------
        List[str]
            Liste des author_id uniques.
        """
        author_ids = list({t.get("author_id") for t in tweets if t.get("author_id")})
        logger.debug(f"Author IDs uniques extraits: {len(author_ids)} users")
        return author_ids

    def reset_counters(self) -> None:
        """
        Réinitialise les compteurs de budget.
        À utiliser avec précaution — ne réinitialise pas le vrai compteur API X.
        """
        self._tweets_fetched = 0
        logger.info("Compteurs budget réinitialisés")
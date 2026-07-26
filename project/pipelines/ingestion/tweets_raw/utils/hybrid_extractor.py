# src/extract/hybrid_extractor.py
"""
HybridExtractor — Orchestrateur qui combine API X + Scraping twikit.

Stratégie hybride :
  - API X (pay-per-use, 0.005$/tweet) → collecte les tweets bruts
  - twikit (gratuit) → enrichit les profils utilisateurs
  - Budget max : 25$ = ~5000 tweets
  - Limite par collecte : 250 tweets max

Architecture :
    HybridExtractor.run()
        ├── APIExtractor.search_tweets()      → tweets bruts (250 max)
        ├── APIExtractor.get_author_ids()     → author_id uniques
        ├── ScraperEnricher.enrich_batch()    → profils users enrichis
        └── merge() → tweets enrichis complets
            └── Validator + Serializer + Producer (Kafka)

Usage
-----
>>> extractor = HybridExtractor(
...     bearer_token="XXX",
...     twikit_auth_token="YYY",
...     budget_limit=25.0
... )
>>> result = extractor.run(keywords="Ligue1 OR PSG OR OM")
>>> print(f"{result['tweets_enriched']} tweets enrichis envoyés à Kafka")
"""

import logging
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from .api_extractor import (
    APIExtractor,
    SearchConfig,
    APIError,
    BudgetExceededError,
)
from .scraper_enricher import (
    ScraperEnricher,
    ScraperNotInitializedError,
    UserProfile,
)
from .tweet_validator import TweetValidator


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────

@dataclass
class HybridConfig:
    """
    Configuration complète de l'extracteur hybride.

    Regroupe les paramètres API X + scraping + pipeline.
    """
    # --- API X ---
    bearer_token: str
    budget_limit: float = 25.0
    keywords: str = "Ligue1"
    max_results: int = 250
    lang: Optional[str] = "fr"
    min_retweets: Optional[int] = None
    start_time: Optional[str] = None

    # --- Scraping (twikit) ---
    twikit_auth_token: Optional[str] = None
    twikit_username: Optional[str] = None
    twikit_email: Optional[str] = None
    twikit_password: Optional[str] = None
    scrape_user_profiles: bool = True  # Désactiver si on veut juste les tweets
    scraper_delay_seconds: float = 1.5

    # --- Pipeline ---
    enrich_only_known_users: bool = False  # Skip users non scrapés
    skip_validation: bool = False
    skip_kafka: bool = False  # Mode test sans Kafka

    def __post_init__(self):
        if not self.bearer_token or not self.bearer_token.strip():
            raise ValueError("bearer_token X API est requis")

        if self.budget_limit <= 0:
            raise ValueError("budget_limit doit être > 0")

        if self.max_results <= 0 or self.max_results > 250:
            raise ValueError("max_results doit être entre 1 et 250")


# ─────────────────────────────────────────────────────────────────────
# RESULT
# ─────────────────────────────────────────────────────────────────────

@dataclass
class ExtractionResult:
    """
    Résultat d'une collecte hybride complète.

    Retourne des statistiques détaillées pour le monitoring.
    """
    success: bool
    tweets_fetched: int = 0
    users_enriched: int = 0
    tweets_enriched: int = 0
    tweets_valid: int = 0
    tweets_invalid: int = 0
    tweets_sent_to_kafka: int = 0
    api_cost_estimate: float = 0.0
    remaining_budget: float = 0.0
    author_ids_found: int = 0
    errors: List[str] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.metadata is None:
            self.metadata = {}

    def to_dict(self) -> Dict[str, Any]:
        """Retourne un dict sérialisable (pour logging/Kafka)."""
        return {
            "success": self.success,
            "tweets_fetched": self.tweets_fetched,
            "users_enriched": self.users_enriched,
            "tweets_enriched": self.tweets_enriched,
            "tweets_valid": self.tweets_valid,
            "tweets_invalid": self.tweets_invalid,
            "tweets_sent_to_kafka": self.tweets_sent_to_kafka,
            "api_cost_estimate": round(self.api_cost_estimate, 2),
            "remaining_budget": round(self.remaining_budget, 2),
            "author_ids_found": self.author_ids_found,
            "errors": self.errors,
            "metadata": self.metadata or {},
        }


# ─────────────────────────────────────────────────────────────────────
# HYBRID EXTRACTOR
# ─────────────────────────────────────────────────────────────────────

class HybridExtractor:
    """
    Orchestrateur qui combine API X + Scraping twikit pour une
    collecte complète de tweets enrichis avec profils utilisateurs.

    Workflow complet :
        1. search_tweets() — API X → tweets bruts (author_id, text, created_at...)
        2. enrich_users() — twikit → profils (username, followers, verified...)
        3. merge() — combine tweets + profils → tweets enrichis
        4. validate() — filtre les tweets valides
        5. serialize() — formate pour Kafka
        6. send_to_kafka() — envoie au producer

    Usage
    -----
    >>> extractor = HybridExtractor(
    ...     bearer_token="X_API_TOKEN",
    ...     twikit_auth_token="TWIKIT_AUTH",
    ...     budget_limit=25.0
    ... )
    >>> result = extractor.run(keywords="PSG OR OM")
    >>> print(result.to_dict())

    Mode sans scraping (API X seulement) :
    >>> extractor = HybridExtractor(
    ...     bearer_token="X_API_TOKEN",
    ...     scrape_user_profiles=False
    ... )
    >>> result = extractor.run()
    >>> # Les tweets auront les champs author_id mais pas les profils enrichis
    """

    def __init__(
        self,
        bearer_token: str,
        twikit_auth_token: Optional[str] = None,
        twikit_username: Optional[str] = None,
        twikit_email: Optional[str] = None,
        twikit_password: Optional[str] = None,
        budget_limit: float = 25.0,
        keywords: str = "Ligue1",
        max_results: int = 250,
        lang: Optional[str] = "fr",
        scrape_user_profiles: bool = True,
        scraper_delay_seconds: float = 1.5,
    ):
        """
        Initialise l'extracteur hybride.

        Parameters
        ----------
        bearer_token : str
            Bearer token API X v2.
        twikit_auth_token : Optional[str]
            Auth token twikit (cookie auth_token).
        twikit_username : Optional[str]
            Username du compte X pour twikit.
        twikit_email : Optional[str]
            Email du compte X (si login par mdp).
        twikit_password : Optional[str]
            Mot de passe du compte X (si login par mdp).
        budget_limit : float
            Budget max en dollars (défaut: 25$).
        keywords : str
            Mots-clés de recherche.
        max_results : int
            Max tweets par collecte (capé à 250).
        lang : Optional[str]
            Filtre de langue.
        scrape_user_profiles : bool
            Si True, enrichit les profils via twikit.
        scraper_delay_seconds : float
            Délai entre chaque appel scraping.
        """
        # Config
        self._config = HybridConfig(
            bearer_token=bearer_token,
            twikit_auth_token=twikit_auth_token,
            twikit_username=twikit_username,
            twikit_email=twikit_email,
            twikit_password=twikit_password,
            budget_limit=budget_limit,
            keywords=keywords,
            max_results=max_results,
            lang=lang,
            scrape_user_profiles=scrape_user_profiles,
            scraper_delay_seconds=scraper_delay_seconds,
        )

        # Composants
        self._api_extractor: Optional[APIExtractor] = None
        self._scraper_enricher: Optional[ScraperEnricher] = None

        # Pipeline (injectables)
        self._validator = TweetValidator()
        self._serializer = None
        self._producer = None
        self._s3_writer = None

        # État interne
        self._raw_tweets: List[Dict[str, Any]] = []
        self._user_profiles: Dict[str, UserProfile] = {}
        self._enriched_tweets: List[Dict[str, Any]] = []
        self._errors: List[str] = []

        logger.info(
            f"HybridExtractor initialisé — Budget: {budget_limit}$, "
            f"max_results: {max_results}, "
            f"scrape_profiles: {scrape_user_profiles}"
        )

    # ─────────────────────────────────────────────────────────────────
    # PUBLIC API — RUN
    # ─────────────────────────────────────────────────────────────────

    def set_pipeline(
        self,
        validator=None,
        serializer=None,
        producer=None,
        s3_writer=None,
    ) -> None:
        """
        Injecte les composants du pipeline (validator, serializer, producer, s3_writer).

        Permet l'injection de dépendances pour les tests et la flexibilité.
        """
        self._validator = validator
        self._serializer = serializer
        self._producer = producer
        self._s3_writer = s3_writer

    def run(
        self,
        keywords: Optional[str] = None,
        max_results: Optional[int] = None
    ) -> ExtractionResult:
        """
        Exécute une collecte hybride complète.

        Workflow synchrone :
        1. Cherche les tweets via API X
        2. Extrait les author_id
        3. Enrichit les profils via twikit
        4. Merge tweets + profils
        5. Valide, sérialise, envoie à Kafka

        Parameters
        ----------
        keywords : Optional[str]
            Surcharge les keywords de la config.
        max_results : Optional[int]
            Surcharge le max_results de la config.

        Returns
        -------
        ExtractionResult
            Statistiques détaillées de la collecte.
        """
        logger.info("=" * 60)
        logger.info("DÉMARRAGE COLLECTE HYBRIDE")
        logger.info("=" * 60)

        result = ExtractionResult(
            success=False,
            errors=self._errors,
            metadata={"started_at": datetime.now(timezone.utc).isoformat()}
        )

        try:
            # ─── Étape 1 : Initialiser les composants ───────────────
            self._init_components(keywords, max_results)

            # ─── Étape 2 : API X → tweets bruts ──────────────────────
            logger.info("[1/5] Recherche tweets via API X...")
            self._raw_tweets = self._api_extractor.search_tweets()

            if not self._raw_tweets:
                logger.warning("Aucun tweet trouvé pour cette recherche")
                result.metadata["finished_at"] = datetime.now(timezone.utc).isoformat()
                result.success = True  # Pas une erreur, juste vide
                return result

            result.tweets_fetched = len(self._raw_tweets)
            result.author_ids_found = len(
                set(t.get("author_id") for t in self._raw_tweets if t.get("author_id"))
            )
            logger.info(
                f"[1/5] {result.tweets_fetched} tweets collectés, "
                f"{result.author_ids_found} auteurs uniques"
            )

            # ─── Étape 3 : twikit → profils utilisateurs ─────────────
            if self._config.scrape_user_profiles:
                logger.info("[2/5] Enrichissement profils via twikit...")
                self._user_profiles = self._enrich_users()
                result.users_enriched = len(self._user_profiles)
                logger.info(
                    f"[2/5] {result.users_enriched} profils enrichis "
                    f"(cache hit rate: {self._scraper_enricher._cache.hit_rate:.1%})"
                )
            else:
                logger.info("[2/5] Enrichissement profils SKIP (scrape_user_profiles=False)")

            # ─── Étape 4 : Merge tweets + profils ────────────────────
            logger.info("[3/5] Fusion tweets + profils...")
            self._enriched_tweets = self._merge()
            result.tweets_enriched = len(self._enriched_tweets)
            logger.info(f"[3/5] {result.tweets_enriched} tweets enrichis")

            # Buffer S3 : tous les tweets enrichis (avant filtrage validation)
            if self._s3_writer is not None:
                self._s3_writer.write_batch(self._enriched_tweets)

            # ─── Étape 5 : Validation ────────────────────────────────
            logger.info("[4/5] Validation...")
            validated = self._validate()
            result.tweets_valid = validated["valid"]
            result.tweets_invalid = validated["invalid"]
            logger.info(
                f"[4/5] Validation: {result.tweets_valid} valides, "
                f"{result.tweets_invalid} invalides"
            )

            # ─── Étape 6 : Sérialisation + Kafka ─────────────────────
            if not self._config.skip_kafka:
                logger.info("[5/5] Sérialisation + Kafka...")
                sent = self._send_to_kafka(validated["tweets"])
                result.tweets_sent_to_kafka = sent
                logger.info(f"[5/5] {sent} tweets envoyés à Kafka")
            else:
                logger.info("[5/5] Kafka SKIP (mode test)")

            # ─── Succès ──────────────────────────────────────────────
            result.success = True
            result.api_cost_estimate = self._api_extractor.estimated_cost
            result.remaining_budget = self._api_extractor.remaining_budget
            result.metadata["finished_at"] = datetime.now(timezone.utc).isoformat()

        except BudgetExceededError as e:
            self._errors.append(f"BUDGET: {e}")
            logger.error(self._errors[-1])
            result.errors = self._errors

        except ScraperNotInitializedError as e:
            self._errors.append(f"SCRAPER: {e}")
            logger.error(self._errors[-1])
            result.errors = self._errors
            # On continue quand même avec les tweets non enrichis

        except APIError as e:
            self._errors.append(f"API: {e}")
            logger.error(self._errors[-1])
            result.errors = self._errors

        except Exception as e:
            self._errors.append(f"INATTENDU: {e}")
            logger.exception("Erreur inattendue dans HybridExtractor.run()")
            result.errors = self._errors

        finally:
            if self._s3_writer is not None:
                s3_count = self._s3_writer.flush()
                if s3_count:
                    logger.info(f"  S3 backup: {s3_count} tweets écrits")

            logger.info("=" * 60)
            logger.info(f"COLLECTE TERMINÉE — Success: {result.success}")
            if result.tweets_fetched > 0:
                logger.info(
                    f"  Tweets: {result.tweets_fetched} fetchés, "
                    f"{result.tweets_enriched} enrichis, "
                    f"{result.tweets_valid} valides"
                )
            logger.info(
                f"  Coût API: ~{result.api_cost_estimate:.2f}$, "
                f"Reste: ~{result.remaining_budget:.2f}$"
            )
            logger.info("=" * 60)

        return result

    # ─────────────────────────────────────────────────────────────────
    # ÉTAPES INTERNES
    # ─────────────────────────────────────────────────────────────────

    def _init_components(
        self,
        keywords: Optional[str],
        max_results: Optional[int]
    ) -> None:
        """Initialise APIExtractor et ScraperEnricher."""
        # API Extractor
        self._api_extractor = APIExtractor(
            bearer_token=self._config.bearer_token,
            config=SearchConfig(
                keywords=keywords or self._config.keywords,
                max_results=max_results or self._config.max_results,
                lang=self._config.lang,
                start_time=self._config.start_time,
                min_retweets=self._config.min_retweets,
            ),
            budget_limit=self._config.budget_limit
        )

        # Scraper Enricher
        if self._config.scrape_user_profiles:
            self._scraper_enricher = ScraperEnricher(
                cache_size=500,
                delay_seconds=self._config.scraper_delay_seconds,
            )
            # Login twikit si credentials fournis
            if self._config.twikit_auth_token:
                asyncio.run(self._scraper_enricher.login(
                    auth_token=self._config.twikit_auth_token
                ))
            elif self._config.twikit_email and self._config.twikit_password:
                asyncio.run(self._scraper_enricher.login(
                    email=self._config.twikit_email,
                    password=self._config.twikit_password
                ))
            else:
                logger.warning(
                    "Aucun token d'authentification twikit fourni — "
                    "le scraper ne pourra pas enrichir les profils"
            )

    def _merge(self) -> List[Dict[str, Any]]:
        """
        Fusionne les tweets bruts avec les profils utilisateurs enrichis.

        Returns
        -------
        List[Dict[str, Any]]
            Liste des tweets enrichis (profils + tweets bruts).
        """
        if not self._raw_tweets:
            return []

        enriched = []

        for tweet in self._raw_tweets:
            author_id = tweet.get("author_id")

            if author_id and author_id in self._user_profiles:
                # Profil trouvé → fusionne
                profile = self._user_profiles[author_id]
                enriched_tweet = profile.to_enriched_tweet(tweet)
                enriched.append(enriched_tweet)
            else:
                # Profil non trouvé → tweet brut sans enrichissement
                enriched.append(dict(tweet))

            logger.debug(f"Tweet {tweet.get('id')} enrichi: {author_id in self._user_profiles}")

        return enriched
    
    def _validate(self) -> Dict[str, Any]:
        """
        Valide les tweets enrichis via TweetValidator.

        Returns
        -------
        Dict[str, Any]
            {
                "valid": int,           # nombre de tweets valides
                "invalid": int,         # nombre de tweets invalides
                "tweets": List[Dict],   # tweets valides
            }
        """
        if not self._enriched_tweets:
            return {"valid": 0, "invalid": 0, "tweets": []}

        valid_tweets = []
        invalid_count = 0

        for tweet in self._enriched_tweets:
            if self._validator.validate(tweet):
                valid_tweets.append(tweet)
            else:
                invalid_count += 1

        return {
            "valid": len(valid_tweets),
            "invalid": invalid_count,
            "tweets": valid_tweets,
        }

    def _send_to_kafka(self, tweets: List[Dict[str, Any]]) -> int:
        """
        Sérialise et envoie les tweets valides à Kafka via le producer.

        Parameters
        ----------
        tweets : List[Dict[str, Any]]
            Liste des tweets valides à envoyer.

        Returns
        -------
        int
            Nombre de tweets effectivement envoyés.
        """
        if not tweets:
            logger.debug("_send_to_kafka: aucun tweet à envoyer")
            return 0

        if self._producer is None:
            logger.warning("_send_to_kafka: producer non initialisé — skip Kafka")
            return 0

        sent_count = 0

        for tweet in tweets:
            try:
                # Le producer attend un dict tweet, pas des bytes
                # Il gère lui-même la validation, sérialisation et l'envoi
                self._producer.send(tweet)
                sent_count += 1
                self._producer._producer.poll(0)  # Trigger delivery callbacks
            except Exception as e:
                logger.warning(f"Échec envoi tweet {tweet.get('id')}: {e}")

        logger.info(f"Producer type: {type(self._producer).__name__}")
        logger.info(f"Producer module: {self._producer.__class__.__module__}")

        remaining = self._producer.flush(timeout=120)
        if remaining > 0:
            logger.error(f"FLUSH INCOMPLET: {remaining} messages non livrés")

        logger.info(f"_send_to_kafka: {sent_count}/{len(tweets)} tweets envoyés")
        return sent_count

    def _enrich_users(self) -> Dict[str, UserProfile]:
        """Enrichit les profils des auteurs via twikit."""
        if not self._scraper_enricher:
            return {}

        # Extrait les usernames depuis les tweets bruts
        # L'API X retourne author_id mais pas le username directement
        # On enrichit par author_id si disponible
        author_ids = list(set(
            t.get("author_id")
            for t in self._raw_tweets
            if t.get("author_id")
        ))

        if not author_ids:
            return {}

        # twikit enrichit par username, pas par author_id
        # Si tu n'as pas les usernames dans les tweets bruts,
        # retourne un dict vide — l'enrichissement sera skip
        usernames = list(set(
            t.get("author_username") or t.get("username")
            for t in self._raw_tweets
            if t.get("author_username") or t.get("username")
        ))

        if not usernames:
            logger.warning("Aucun username trouvé dans les tweets bruts — enrichissement impossible")
            return {}

        profiles_list = asyncio.run(
            self._scraper_enricher.enrich_batch(usernames)
        )

        return {p.user_id: p for p in profiles_list if p}
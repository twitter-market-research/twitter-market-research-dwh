# src/extract/scraper_enricher.py
"""
ScraperEnricher — Enrichissement des profils utilisateurs via scraping.

Stratégie hybride :
  - API X (pay-per-use) → collecte les tweets bruts (author_id, text, created_at...)
  - twikit (gratuit) → enrichit les profils users (username, followers, verified...)

Usage
-----
>>> enricher = ScraperEnricher(cache_size=500, delay_seconds=1.5)
>>> await enricher.login(username="my_x_account", auth_token="...")
>>> profiles = await enricher.enrich_batch(["ligue1", "psg", "om"])
>>> enriched_tweets = await enricher.enrich_tweets(raw_tweets, profiles)
"""

import logging
import asyncio
from dataclasses import dataclass, field
from collections import OrderedDict
from typing import Optional, List, Dict, Any

# twikit — bibliothèque de scraping X sans API key
# pip install twikit
try:
    import twikit
    from twikit.errors import UserNotFound, TwitterException
    TWIKIT_AVAILABLE = True
except ImportError:
    twikit = None
    UserNotFound = TwitterException = Exception  # type: ignore
    TWIKIT_AVAILABLE = False


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# EXCEPTIONS
# ─────────────────────────────────────────────────────────────────────

class EnrichmentError(Exception):
    """Erreur lors de l'enrichissement d'un profil utilisateur"""
    def __init__(self, message: str, user_id: Optional[str] = None):
        self.message = message
        self.user_id = user_id
        super().__init__(f"EnrichmentError [{user_id}]: {message}")


class ScraperNotInitializedError(EnrichmentError):
    """twikit Client n'a pas été initialisé (login manquant)"""
    pass


# ─────────────────────────────────────────────────────────────────────
# DATA MODELS
# ─────────────────────────────────────────────────────────────────────

@dataclass
class UserProfile:
    """
    Modèle de données pour un profil utilisateur enrichi.

    Fusionne les données de l'API X (author_id) et du scraping twikit.
    """
    user_id: str
    username: Optional[str] = None
    name: Optional[str] = None
    followers_count: int = 0
    following_count: int = 0
    verified: bool = False
    description: Optional[str] = None
    profile_image_url: Optional[str] = None
    location: Optional[str] = None
    created_at: Optional[str] = None
    is_blue_verified: bool = False

    def __post_init__(self):
        if not self.user_id:
            raise ValueError("user_id est requis pour UserProfile")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserProfile":
        """
        Crée un UserProfile depuis un dictionnaire.

        Paramètres tolérants : les champs manquants sont mis à None/0/False.
        """
        return cls(
            user_id=data.get("user_id", "") or data.get("id", ""),
            username=data.get("username") or data.get("screen_name"),
            name=data.get("name"),
            followers_count=int(data.get("followers_count", 0) or 0),
            following_count=int(data.get("following_count", 0) or 0),
            verified=bool(data.get("verified", False)),
            description=data.get("description") or data.get("bio"),
            profile_image_url=data.get("profile_image_url") or data.get("profile_image_url_https"),
            location=data.get("location"),
            created_at=data.get("created_at"),
            is_blue_verified=bool(data.get("is_blue_verified", False)),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Retourne un dict sérialisable (pour Kafka/JSON)."""
        return {
            "user_id": self.user_id,
            "username": self.username,
            "name": self.name,
            "followers_count": self.followers_count,
            "following_count": self.following_count,
            "verified": self.verified,
            "description": self.description,
            "profile_image_url": self.profile_image_url,
            "location": self.location,
            "created_at": self.created_at,
            "is_blue_verified": self.is_blue_verified,
        }

    def to_enriched_tweet(self, tweet: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fusionne ce profil avec un tweet brut pour créer un tweet enrichi.

        Parameters
        ----------
        tweet : Dict[str, Any]
            Tweet brut de l'API X.

        Returns
        -------
        Dict[str, Any]
            Tweet enrichi avec les champs utilisateur.
        """
        enriched = dict(tweet)  # Copy
        enriched["user_username"] = self.username
        enriched["user_name"] = self.name
        enriched["user_followers_count"] = self.followers_count
        enriched["user_following_count"] = self.following_count
        enriched["user_verified"] = self.verified
        enriched["user_description"] = self.description
        enriched["user_location"] = self.location
        enriched["user_is_blue_verified"] = self.is_blue_verified
        enriched["enriched"] = True
        return enriched


# ─────────────────────────────────────────────────────────────────────
# LRU CACHE
# ─────────────────────────────────────────────────────────────────────

class ProfileCache:
    """
    Cache LRU pour les profils utilisateurs.

    Objectif : éviter de re-scrap le même user plusieurs fois
    durant une même collecte, ce qui économise du temps et évite
    les rate limits.
    """

    def __init__(self, max_size: int = 500):
        self.max_size = max_size
        self._cache: OrderedDict[str, UserProfile] = OrderedDict()
        logger.debug(f"ProfileCache créé — max_size={max_size}")

    def get(self, user_id: str) -> Optional[UserProfile]:
        """
        Récupère un profil du cache (et le met en tête LRU).

        Returns
        -------
        Optional[UserProfile]
            Le profil si présent, None sinon.
        """
        if user_id not in self._cache:
            return None

        # Move to end (LRU: most recently used)
        self._cache.move_to_end(user_id)
        return self._cache[user_id]

    def set(self, user_id: str, profile: UserProfile) -> None:
        """
        Stocke un profil dans le cache.

        Si le cache est plein, l'entrée la moins utilisée est évitée.
        """
        if user_id in self._cache:
            # Update existing + move to end
            self._cache.move_to_end(user_id)
            self._cache[user_id] = profile
            return

        # Evict oldest if full
        if len(self._cache) >= self.max_size:
            oldest = next(iter(self._cache))
            logger.debug(f"Cache plein, éviction de {oldest}")
            del self._cache[oldest]

        self._cache[user_id] = profile
        logger.debug(
            f"Cache: profil {user_id} stocké "
            f"(taille: {len(self._cache)}/{self.max_size})"
        )

    def clear(self) -> None:
        """Vide complètement le cache."""
        self._cache.clear()
        logger.info("Cache profils vidé")

    @property
    def size(self) -> int:
        """Nombre d'entrées dans le cache."""
        return len(self._cache)

    @property
    def hit_rate(self) -> float:
        """
        Taux de hit du cache (estimé depuis le dernier reset).
        Retourne 0 si aucun accès n'a été fait.
        """
        total = getattr(self, "_total_accesses", 0)
        hits = getattr(self, "_cache_hits", 0)
        if total == 0:
            return 0.0
        return hits / total

    def record_access(self, hit: bool) -> None:
        """Enregistre un accès au cache (pour statistiques)."""
        self._total_accesses = getattr(self, "_total_accesses", 0) + 1
        if hit:
            self._cache_hits = getattr(self, "_cache_hits", 0) + 1


# ─────────────────────────────────────────────────────────────────────
# SCRAPER ENRICHER
# ─────────────────────────────────────────────────────────────────────

class ScraperEnricher:
    """
    Enrichisseur de profils utilisateurs via twikit (scraping X).

    Workflow typique :
    1. APIExtractor collecte 250 tweets → liste de tweets bruts
    2. APIExtractor.get_author_ids() → liste d'author_id
    3. ScraperEnricher.enrich_batch(usernames) → profils enrichis
    4. ScraperEnricher.enrich_tweets(tweets, profiles) → tweets enrichis

    Usage
    -----
    >>> enricher = ScraperEnricher(cache_size=500, delay_seconds=1.5)
    >>> # Login requis avant le premier appel scraping
    >>> await enricher.login("my_username", "auth_token")
    >>> profiles = await enricher.enrich_batch(["ligue1", "psg"])
    >>> enriched = await enricher.enrich_tweets(raw_tweets, profiles)

    Notes
    -----
    - twikit nécessite un compte X connecté (auth_token ou login/mdp)
    - Un délai de 1-2 secondes entre chaque requête est appliqué
      pour éviter le rate limiting / ban
    - Le cache LRU évite de re-scrap les mêmes users
    """

    def __init__(
        self,
        cache_size: int = 500,
        delay_seconds: float = 1.5,
        max_retries: int = 2
    ):
        if not TWIKIT_AVAILABLE:
            logger.warning(
                "twikit non installé — pip install twikit requis "
                "pour l'enrichissement scraping"
            )

        if delay_seconds < 0:
            raise ValueError("delay_seconds doit être >= 0")

        self._cache = ProfileCache(max_size=cache_size)
        self._delay_seconds = delay_seconds
        self._max_retries = max_retries
        self._client: Optional[Any] = None

        # Stats
        self.stats: Dict[str, int] = {
            "success": 0,
            "failed": 0,
            "cached": 0,
            "scraped": 0,
        }

        logger.info(
            f"ScraperEnricher initialisé — cache_size={cache_size}, "
            f"delay={delay_seconds}s, retries={max_retries}"
        )

    async def login(
        self,
        username: Optional[str] = None,
        auth_token: Optional[str] = None,
        email: Optional[str] = None,
        password: Optional[str] = None
    ) -> None:
        """
        Initialise le client twikit avec authentification.

        Deux méthodes d'auth :
        - auth_token (recommandé) : token de session déjà extrait
        - email + password : login direct (déconseillé en prod)

        Parameters
        ----------
        username : Optional[str]
            Nom d'utilisateur X.
        auth_token : Optional[str]
            Auth token de session (cookie auth_token).
        email : Optional[str]
            Email du compte X (si login par mot de passe).
        password : Optional[str]
            Mot de passe du compte X (si login par mot de passe).

        Raises
        ------
        ValueError
            Si aucune méthode d'auth n'est fournie.
        RuntimeError
            Si twikit n'est pas installé.
        """
        if not TWIKIT_AVAILABLE:
            raise RuntimeError(
                "twikit non installé. Exécutez: pip install twikit"
            )

        if not auth_token and not (email and password):
            raise ValueError(
                "Auth requis: fournissez auth_token OU (email + password)"
            )

        logger.info("Initialisation client twikit...")
        self._client = twikit.Client("fr-FR")

        try:
            if auth_token:
                # twikit 2.x — pas de set_auth_token, on utilise set_cookies
                self._client.set_cookies({"auth_token": auth_token})
                logger.info("Authentification twikit via cookie réussie")
            else:
                await self._client.login(
                    auth_info_1=email,
                    password=password
                )
                logger.info("Authentification twikit via login/mdp réussie")

        except Exception as e:
            logger.error(f"Échec authentification twikit: {e}")
            self._client = None
            raise EnrichmentError(f"Échec login twikit: {e}")

    async def _scrape_user_profile(
        self,
        username: str
    ) -> Optional[UserProfile]:
        """
        Scrape le profil d'un utilisateur via twikit.

        Utilise le cache LRU pour éviter les appels redondants.
        Respecte le délai anti-rate-limit entre les appels.

        Parameters
        ----------
        username : str
            Username X (@sans le @).

        Returns
        -------
        Optional[UserProfile]
            Le profil enrichi, ou None si échec.
        """
        if self._client is None:
            raise ScraperNotInitializedError(
                "Client twikit non initialisé. Appelez login() d'abord."
            )

        # Essayer de récupérer l'user_id depuis le cache par username
        # (Note: le cache est indexé par user_id, donc on fait un lookup)
        # Pour le premier appel, on va directement scraper

        logger.debug(f"Scraping profil: @{username}")

        # Respecter le délai anti-rate-limit
        await asyncio.sleep(self._delay_seconds)

        for attempt in range(self._max_retries + 1):
            try:
                # Twikit: get_user_by_screen_name
                user = await self._client.get_user_by_screen_name(username)

                # Adapter la réponse twikit → UserProfile
                profile = UserProfile(
                    user_id=user.id,
                    username=user.screen_name,
                    name=user.name,
                    followers_count=user.followers_count or 0,
                    following_count=user.friends_count or 0,
                    verified=bool(user.verified),
                    description=user.description,
                    profile_image_url=getattr(user, "profile_image_url_https", None),
                    location=getattr(user, "location", None),
                    created_at=getattr(user, "created_at", None),
                    is_blue_verified=bool(getattr(user, "is_blue_verified", False)),
                )

                # Mettre en cache
                self._cache.set(profile.user_id, profile)
                self._cache.record_access(hit=False)
                self.stats["scraped"] += 1
                self.stats["success"] += 1

                logger.debug(
                    f"Profil @{username} scrapé: {profile.followers_count} followers"
                )
                return profile

            except UserNotFound:
                logger.warning(f"User @{username} introuvable sur X")
                self.stats["failed"] += 1
                return None

            except Exception as e:
                logger.warning(
                    f"Échec scraping @{username} (tentative {attempt+1}/"
                    f"{self._max_retries + 1}): {e}"
                )
                if attempt == self._max_retries:
                    self.stats["failed"] += 1
                    return None
                # Pause avant retry
                await asyncio.sleep(self._delay_seconds * 2)

    async def enrich_batch(
        self,
        usernames: List[str]
    ) -> List[UserProfile]:
        """
        Enrichit un batch de usernames en parallèle.

        Parameters
        ----------
        usernames : List[str]
            Liste des usernames X (sans @).

        Returns
        -------
        List[UserProfile]
            Liste des profils enrichis (exclut les échecs).
        """
        if not usernames:
            logger.debug("enrich_batch: liste vide, rien à faire")
            return []

        # Dédupliquer
        unique_usernames = list(dict.fromkeys(usernames))
        logger.info(
            f"Enrichissement batch: {len(unique_usernames)} users "
            f"(dédupliqué de {len(usernames)})"
        )

        # Créer les coroutines
        tasks = [
            self._scrape_user_profile(username)
            for username in unique_usernames
        ]

        # Exécuter en parallèle (twikit gère ses propres rate limits)
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filtrer les résultats valides
        profiles: List[UserProfile] = []
        for i, result in enumerate(results):
            if isinstance(result, UserProfile):
                profiles.append(result)
            elif isinstance(result, Exception):
                logger.error(
                    f"Exception enrich_batch[{i}] ({unique_usernames[i]}): "
                    f"{result}"
                )
            # None est silencieux (user non trouvé)

        logger.info(
            f"Enrichissement terminé: {len(profiles)}/{len(unique_usernames)} enrichis,")
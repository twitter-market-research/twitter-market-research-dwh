# tests/test_hybrid_extractor.py
from unittest.mock import MagicMock

from project.pipelines.ingestion.tweets_raw.utils.hybrid_extractor import (
    HybridExtractor,
)
from project.pipelines.ingestion.tweets_raw.utils.tweet_validator import (
    ValidationResult,
)


class TestHybridExtractorValidation:
    """Couvre le comptage valides/invalides de _validate() (bug A).

    ``TweetValidator.validate`` renvoie un ``ValidationResult`` (dataclass),
    toujours truthy : tester la vérité de l'objet au lieu de ``.is_valid``
    faisait passer 100% des tweets pour valides.
    """

    def _make_extractor(self, validator):
        extractor = HybridExtractor(
            bearer_token="FAKE",
            scrape_user_profiles=False,
        )
        extractor.set_pipeline(validator=validator)
        return extractor

    def test_validate_separates_valid_from_invalid(self):
        """Un tweet dont is_valid=False doit être compté invalide et exclu."""
        def fake_validate(tweet):
            ok = tweet["ok"]
            return ValidationResult(
                is_valid=ok,
                errors=[] if ok else ["boom"],
            )

        validator = MagicMock()
        validator.validate.side_effect = fake_validate
        extractor = self._make_extractor(validator)
        extractor._enriched_tweets = [
            {"ok": True, "id": "1"},
            {"ok": False, "id": "2"},
            {"ok": True, "id": "3"},
        ]

        result = extractor._validate()

        assert result["valid"] == 2
        assert result["invalid"] == 1
        assert [t["id"] for t in result["tweets"]] == ["1", "3"]


# RED 1 — merge des données API + scraping
class TestHybridExtractorMerge:
    def test_merge_enriches_tweet_with_user_profile(self):
        """Un tweet API + profil twikit = tweet enrichi complet"""


# RED 2 — caching des profils utilisateurs
class TestScraperEnricherCache:
    def test_enrich_user_profiles_caches_results(self):
        """Les profils users sont mis en cache pour éviter re-scraping"""


# RED 3 — fallback si twikit échoue
class TestScraperEnricherFallback:
    def test_enrich_falls_back_gracefully_on_scrape_error(self):
        """Si twikit plante, le tweet garde ses données API uniquement"""

# RED 4 — rate limiting du scraper
class TestScraperEnricherRateLimit:
    def test_enrich_respects_rate_limit_between_requests(self):
        """Un délai est respecté entre chaque appel twikit"""
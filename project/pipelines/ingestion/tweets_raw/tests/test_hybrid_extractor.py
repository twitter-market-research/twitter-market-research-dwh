# tests/test_hybrid_extractor.py

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
#!/usr/bin/env python3
"""
extract.py — Point d'entrée unique pour lancer la pipeline de collecte.

Usage
-----
# 1. Collecte simple (API X + scraping activé)
python extract.py --keywords "PSG OR OM OR Ligue1" --max-results 250

# 2. Collecte sans scraping (API X seulement, plus rapide)
python extract.py --keywords "Ligue1" --no-scrape

# 3. Collecte avec filtre temporel
python extract.py --keywords "Ligue1" --start-time "2026-03-28T00:00:00Z"

# 4. Collecte en mode test (sans Kafka)
python extract.py --keywords "PSG" --skip-kafka

# 5. Collecte via fichier de config
python extract.py --config config/extraction.yaml

# 6. Aide
python extract.py --help
"""

import argparse
import json
import logging
import os
import sys
import signal
from typing import Optional

# Notre pipeline
from utils.hybrid_extractor import HybridExtractor, ExtractionResult
from utils.api_extractor import BudgetExceededError
from utils.tweet_validator import TweetValidator
from utils.tweet_serializer import TweetSerializer
from utils.kafka_producer import TweetsRawProducer
from utils.s3_writer import S3RawWriter


# ─────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────


def setup_logging(verbose: bool = False, log_file: Optional[str] = None):
    """Configure le logging pour la pipeline."""
    level = logging.DEBUG if verbose else logging.INFO

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )


# ─────────────────────────────────────────────────────────────────────
# CONFIGURATION ENV / SECRETS
# ─────────────────────────────────────────────────────────────────────

def get_env_or_fail(var_name: str, default: Optional[str] = None) -> str:
    """Récupère une variable d'environnement ou lève une erreur."""
    value = os.environ.get(var_name, default)
    if not value:
        raise ValueError(
            f"Variable d'environnement requise: {var_name}. "
            f"Définissez-la dans votre shell ou dans un fichier .env"
        )
    return value


def load_secrets() -> dict:
    """
    Charge les secrets depuis les variables d'environnement.

    Variables attendues :
    - X_BEARER_TOKEN      : Token API X v2 (Bearer token)
    - TWIKIT_AUTH_TOKEN   : Auth token twikit (optionnel, pour le scraping)
    - KAFKA_BOOTSTRAP     : Broker Kafka (optionnel, défaut: localhost:9092)
    """
    return {
        "x_bearer_token": get_env_or_fail("TWITTER_BEARER_TOKEN"),
        "twikit_auth_token": os.environ.get("TWIKIT_AUTH_TOKEN"),
        "kafka_bootstrap": os.environ.get("KAFKA_BROKERS", "localhost:9092"),
    }


def load_s3_config() -> Optional[dict]:
    """
    Charge la configuration S3/MinIO depuis les variables d'environnement.

    Retourne None si S3_ENDPOINT_URL n'est pas défini (S3 désactivé).
    """
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    if not endpoint:
        return None
    return {
        "endpoint_url": endpoint,
        "bucket": os.environ.get("S3_BUCKET", "tweets-raw"),
        "aws_access_key_id": os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        "aws_secret_access_key": os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
    }


# ─────────────────────────────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────────────────────────────

def build_extractor(
    secrets: dict,
    keywords: str,
    max_results: int,
    lang: Optional[str],
    scrape_profiles: bool,
    scraper_delay: float,
    skip_kafka: bool,
    skip_s3: bool,
) -> HybridExtractor:
    """
    Construit et configure l'HybridExtractor avec tous ses composants.
    """
    extractor = HybridExtractor(
        bearer_token=secrets["x_bearer_token"],
        twikit_auth_token=secrets.get("twikit_auth_token"),
        budget_limit=25.0,  # Hard budget
        keywords=keywords,
        max_results=max_results,
        lang=lang,
        scrape_user_profiles=scrape_profiles,
        scraper_delay_seconds=scraper_delay,
    )

    s3_writer = None
    if not skip_s3:
        s3_config = load_s3_config()
        if s3_config:
            s3_writer = S3RawWriter(**s3_config)
        else:
            logging.warning("S3_ENDPOINT_URL non défini — backup S3 désactivé")

    extractor.set_pipeline(
        validator=TweetValidator(),
        serializer=TweetSerializer(),
        producer=TweetsRawProducer(
            brokers=secrets["kafka_bootstrap"],
            tweets_topic="tweets_raw",
            audit_topic="audit_logs",
        ) if not skip_kafka else None,
        s3_writer=s3_writer,
    )

    return extractor


def run_pipeline(args) -> int:
    """
    Exécute la pipeline complète.

    Returns
    -------
    int
        Code de sortie (0 = succès, 1 = erreur).
    """
    # ─── 1. Charger les secrets ─────────────────────────────────────
    try:
        secrets = load_secrets()
    except ValueError as e:
        logging.error(f"Configuration manquante: {e}")
        return 1

    # ─── 2. Construire l'extracteur ─────────────────────────────────
    logging.info("Construction de la pipeline...")
    extractor = build_extractor(
        secrets=secrets,
        keywords=args.keywords,
        max_results=args.max_results,
        lang=args.lang,
        scrape_profiles=not args.no_scrape,
        scraper_delay=args.scraper_delay,
        skip_kafka=args.skip_kafka,
        skip_s3=args.skip_s3,
    )

    # ─── 3. Définir le start_time si fourni ─────────────────────────
    if args.start_time:
        extractor._config.start_time = args.start_time

    # ─── 4. Lancer l'extraction ─────────────────────────────────────
    logging.info(
        f"Lancement collecte — Keywords: '{args.keywords}', "
        f"max_results: {args.max_results}, "
        f"lang: {args.lang or 'toutes'}"
    )

    try:
        result: ExtractionResult = extractor.run(
            keywords=args.keywords,
            max_results=args.max_results,
        )
    except BudgetExceededError:
        logging.error("Budget API X dépassé. Arrêtez de collecter.")
        return 1

    # ─── 5. Sauvegarder le résultat ─────────────────────────────────
    result_dict = result.to_dict()
    result_dict["cli_args"] = vars(args)

    # Sortie JSON sur stdout (pour piping / monitoring)
    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as f:
            json.dump(result_dict, f, indent=2, ensure_ascii=False)
        logging.info(f"Résultat sauvegardé dans: {args.output_json}")

    # ─── 6. Résumé ──────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("RÉSUMÉ DE LA COLLECTE")
    print("=" * 60)
    print(f"  Succès:        {'✓' if result.success else '✗'}")
    print(f"  Tweets fetchés:    {result.tweets_fetched}")
    print(f"  Users enrichis:    {result.users_enriched}")
    print(f"  Tweets enrichis:   {result.tweets_enriched}")
    print(f"  Tweets valides:    {result.tweets_valid}")
    print(f"  Tweets invalides:  {result.tweets_invalid}")
    print(f"  Envoyés Kafka:     {result.tweets_sent_to_kafka}")
    print(f"  Coût API estimé:   ~{result.api_cost_estimate:.2f}$")
    print(f"  Budget restant:    ~{result.remaining_budget:.2f}$")
    print(f"  Collectes restantes: ~{int(result.remaining_budget / 0.005 / 250)}")
    print("=" * 60)

    if result.errors:
        print("\nERREURS:")
        for err in result.errors:
            print(f"  ! {err}")

    return 0 if result.success else 1


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    """Parse les arguments CLI."""
    parser = argparse.ArgumentParser(
        description="Pipeline de collecte tweets X pour étude de marché Ligue 1",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples
--------
# Collecte simple sur la Ligue 1
python extract.py --keywords "Ligue1"

# Collecte PSG + OM, 250 tweets max, sans scraping
python extract.py --keywords "PSG OR OM" --max-results 250 --no-scrape

# Collecte avec date de début (depuis 24h)
python extract.py --keywords "Ligue1" \\
    --start-time "$(date -d '24 hours ago' -Iseconds)Z"

# Mode test (sans Kafka)
python extract.py --keywords "Ligue1" --skip-kafka --verbose

# Sauvegarder le résultat JSON
python extract.py --keywords "Ligue1" --output-json results/collecte_001.json
        """
    )

    parser.add_argument(
        "-k", "--keywords",
        type=str,
        default="Ligue1",
        help="Mots-clés de recherche X API (défaut: Ligue1)"
    )
    parser.add_argument(
        "-m", "--max-results",
        type=int,
        default=250,
        help="Max tweets par collecte (1-250, défaut: 250)"
    )
    parser.add_argument(
        "-l", "--lang",
        type=str,
        default="fr",
        help="Filtre de langue (défaut: fr)"
    )
    parser.add_argument(
        "--start-time",
        type=str,
        default=None,
        help="Début de fenêtre temporelle (ISO 8601, ex: 2026-03-28T00:00:00Z)"
    )
    parser.add_argument(
        "--no-scrape",
        action="store_true",
        help="Désactiver le scraping twikit (API X seulement)"
    )
    parser.add_argument(
        "--scraper-delay",
        type=float,
        default=1.5,
        help="Délai entre appels scraping (défaut: 1.5s)"
    )
    parser.add_argument(
        "--skip-kafka",
        action="store_true",
        help="Mode test: ne pas envoyer à Kafka"
    )
    parser.add_argument(
        "--skip-s3",
        action="store_true",
        help="Désactiver le backup S3/MinIO (S3_ENDPOINT_URL requis sinon)"
    )
    parser.add_argument(
        "-o", "--output-json",
        type=str,
        default=None,
        help="Fichier JSON de sortie pour le résultat"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Mode verbeux (debug logs)"
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Fichier de log (ex: logs/extract.log)"
    )

    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()
    setup_logging(verbose=args.verbose, log_file=args.log_file)

    # Graceful shutdown
    def handle_sigint(_signum, _frame):
        logging.info("\nInterruption par l'utilisateur (Ctrl+C)")
        sys.exit(130)

    signal.signal(signal.SIGINT, handle_sigint)

    # Lancer
    sys.exit(run_pipeline(args))


if __name__ == "__main__":
    main()
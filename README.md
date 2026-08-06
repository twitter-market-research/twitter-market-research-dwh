# Twitter Market Research — Data Lakehouse

A real-time data platform that collects, validates, and stores X (Twitter)
tweets related to **Ligue 1** for market research purposes (sentiment &
club engagement).

This repository houses the **data warehouse / lakehouse**—covering the entire
data ingestion, storage, and processing pipeline. **It does not contain
business dashboards**—only technical monitoring for the platform.


---

## Data Flow

![Architecture globale du projet — les 3 dépôts](archi-twitter.png)

Observability: each broker exposes its JMX metrics via HTTP (Prometheus
agent), which are scraped by **Prometheus**, with alerting handled by
**AlertManager**. ---

## Tech Stack

- **Collection**: X API v2 (`requests`) + `twikit` (profile scraping)
- **Streaming**: Apache Kafka (Confluent 7.5, 3 brokers + ZooKeeper)
- **Raw storage**: MinIO (S3-compatible), populated via Kafka Connect (S3 sink)
- **Serving**: MongoDB *(integration pending)*
- **Processing**: Apache Spark *(pending)*
- **Monitoring**: Prometheus + AlertManager + JMX exporter
- **Serialization / validation**: `pydantic`
- **Language**: Python 3.12
- **Docs**: Sphinx (`docs/`)

---

## Repository Structure

```
project/pipelines/
ingestion/
tweets_raw/          # Main pipeline (operational, tested)
utils/             # api_extractor, hybrid_extractor, kafka_producer,
# key_builder, tweet_validator, tweet_serializer,
# s3_writer, scraper_enricher, metrics
tests/             # pytest suite (91 tests)
extract.py         # CLI entry point for collection
audit_logs/          # Rejection pipeline (partial)
processing/            # Spark processing (pending)
dashboard/             # (empty — dashboards reside in repo #3)

iac/dev/                 # Dev infra (intended for IaC repo)
kafka/                 # Kafka cluster + monitoring (Prometheus/AlertManager)
storage/S3/            # MinIO + Kafka Connect (S3 sink)
storage/mongodb/       # MongoDB
ingestion/             # Producer container
spark/                 # Spark (pending)

docs/                    # Sphinx documentation
```

---

## Quick Start

### Prerequisites

- Docker + Docker Compose
- Python 3.12
- An X API v2 Bearer Token (and, optionally, a twikit `auth_token`)

### 1. Configure secrets

Copy the template and fill in the actual values ​​(the actual file is
ignored by git) :

```bash
cp iac/.env.dist iac/.env.dev
# edit iac/.env.dev: TWITTER_BEARER_TOKEN, TWIKIT_AUTH_TOKEN, ...
```

> ⚠️ Never commit actual secrets. Real values ​​go into
> `iac/.env.dev` (git-ignored), not `iac/.env.dist`.

### 2. Launch the infrastructure

```bash
cd iac/dev
docker compose up -d --build
```

This starts: the Kafka cluster (3 brokers + ZooKeeper), Kafka UI, MinIO,
Prometheus, and AlertManager, and creates the topics (`tweets_raw`,
`tweets_enriched`, `audit_logs`).

### 3. Start a collection

```bash
cd project/pipelines/ingestion/tweets_raw
python extract.py --keywords "Ligue1 OR PSG OR OM" --max-results 250
```

Useful options: `--no-scrape` (API only), `--skip-kafka` (test mode),
`--skip-s3`, `--verbose`. See `python extract.py --help`.

---

## Web interfaces

| Service | URL | Role |
|---------|-----|------|
| Kafka UI | http://localhost:8080 | Explore topics & messages |
| Prometheus | http://localhost:9090 | Metrics & alert rules (`/targets`, `/rules`) |
| AlertManager | http://localhost:9095 | Active alerts |
| MinIO (Console) | http://localhost:9001 | Object storage console (S3 API on :9000) |

---

## Tests

The suite uses a dedicated virtual environment.

```bash
python -m venv project/.venv
project/.venv/Scripts/python -m pip install -r \
project/pipelines/ingestion/requirements.txt pytest pytest-cov

# Run the tweets_raw suite
project/.venv/Scripts/python -m pytest project/pipelines/ingestion/tweets_raw/tests/
```

Current status: **91 passing tests** for the `tweets_raw` pipeline.

---

## Conventions

- **TDD**: Test-first (red → green).
- **NumPy docstrings** (`Parameters` / `Returns`) for functions.
- **Commits**: Conventional Commits (`fix:`, `feat:`, `docs:` …).
- **Secrets**: Never hardcoded; always via `iac/.env.dev` (git-ignored).

---

## Project Status

| Component | Status |
|-----------|--------|
| Ingestion `tweets_raw` (collection, validation, Kafka, S3 backup) | ✅ Operational, tested |
| Kafka cluster (3 brokers, topics, retention) | ✅ |
| S3 Sink (Kafka Connect → MinIO) | ✅ Configured |
| Monitoring (Prometheus / AlertManager / JMX) | ✅ Operational |
| `audit_logs` pipeline | 🚧 Partial |
| MongoDB integration (serving) | ⬜ Upcoming |
| Spark processing | ⬜ Upcoming |
| CI/CD (GitLab) | 🚧 Skeleton |

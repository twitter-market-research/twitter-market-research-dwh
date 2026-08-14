# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

The **data platform (lakehouse)** for a Twitter/X market-research project on
French football (Ligue 1). It is repo #1 of a planned **3-repo split**:

1. **this repo** — ingestion, storage, processing + technical monitoring. **No
   business dashboards live here** (only Prometheus/AlertManager for the
   platform itself).
2. `…-iac` — infrastructure-as-code (the `iac/` tree here will migrate there).
3. `…-dashboards` — business BI / visualisation.

Keep that boundary in mind: dashboard/serving concerns belong to the other
repos. `project/pipelines/dashboard/` is intentionally empty.

## Data flow (the big picture)

```
X API v2 (requests) + twikit scraper
        │
        ▼
ingestion producer  ──►  Kafka (3 brokers + ZooKeeper)
                          topics: tweets_raw, tweets_enriched, audit_logs
                          │                         │
                          ▼                         ▼
             Kafka Connect S3 sink       Spark Structured Streaming
                          │              (processing/, IN PROGRESS)
                          ▼                         │
                   MinIO (raw backup,               ▼
                   year=/month=/day=)        BigQuery (emulator in dev)
                                             twitter_staging → twitter_marts
```

- **Kafka message key = `author_id`** (KeyBuilder) so tweets spread evenly
  across the 3 partitions.
- **`audit_logs` is a single multiplexed topic.** Three producers share it and
  are told apart by a Kafka header `audit_type`: `validation_error` (rejected
  tweets), `app_log` (the log-shipper), and the Connect S3-sink DLQ
  (`__connect.errors.*` headers). Consumers route on the header, not the value.
- **Processing "themes" are content topics, not clubs**: `analyse_video`,
  `stats_analytics`, `var`. The study is about an AI video-analysis app, so
  ingestion casts a wide Ligue 1 net and the processing layer tags the relevant
  subset. Target output = the 4 KPIs in `Projet 10-KPI`. Sentiment scoring is
  deferred to a later iteration (themes + engagement first).

## Module layout

- `project/pipelines/ingestion/{tweets_raw,audit_logs}/` — each has
  `extract.py` (CLI entry), `utils/` (pure, unit-tested logic), `tests/`,
  `Dockerfile`. Kafka access uses **confluent-kafka** (migrated off
  kafka-python); the producer is configured **at-least-once**
  (`enable.idempotence`, `acks=all`).
- `project/pipelines/processing/` — `utils/` holds pure logic
  (`theme_tagger`, `enrichment`) kept free of Spark so it tests offline; the
  Spark job wraps it (`schema.py` → `transform.py` → `bq_writer.py` →
  `stream.py`). The BigQuery sink runs in `foreachBatch`.
- `iac/dev/` — one `docker-compose.yml` per service (`kafka/`, `storage/S3/`,
  `storage/mongodb/`, `ingestion/`, `spark/`), orchestrated by
  `iac/dev/docker-compose.yml` via `include`. All services share the external
  network `twitter-research-network`. Kafka brokers run a JMX Prometheus
  javaagent scraped by Prometheus.

## Common commands

**Run the stack** (starts Kafka+ZooKeeper, Kafka UI, MinIO, Kafka Connect,
Prometheus, AlertManager, and creates the topics; spark/dashboard are commented
out in the include):

```bash
cp iac/.env.dist iac/.env.dev    # then fill real tokens — .env.dev is gitignored
cd iac/dev && docker compose up -d --build
```

**Run a collection** (from the pipeline dir):

```bash
cd project/pipelines/ingestion/tweets_raw
python extract.py --keywords "Ligue1 OR PSG OR OM" --max-results 250
# flags: --no-scrape (API only), --skip-kafka, --skip-s3, --verbose
```

**Tests** — a dedicated venv lives at `project/.venv`. Tests import modules by
their **full package path** (`project.pipelines.…`) and rely on implicit
namespace packages (no `__init__.py`), so they **must be run from the repo root
with `python -m pytest`** (which puts the root on `sys.path`):

```bash
# from the repo root
project/.venv/Scripts/python -m pytest project/pipelines/ingestion/tweets_raw/tests/
project/.venv/Scripts/python -m pytest project/pipelines/processing/tests/

# a single test
project/.venv/Scripts/python -m pytest \
    project/pipelines/processing/tests/test_theme_tagger.py::TestTagThemes::test_matches_var_from_text
```

`tests/conftest.py` in `tweets_raw` provides an async-test shim (runs
`async def` tests via `asyncio.run`) because `pytest-asyncio` is not installed.

## Conventions

- **TDD**: write the failing test first, then the implementation.
- **NumPy-style docstrings** (`Parameters` / `Returns`) on functions you touch.
- **Comments and commit messages in English**; **Conventional Commits**
  (`feat:`, `fix:`, `docs:` …).
- **79-column line limit** (flake8) — the IDE linter flags E501; wrap docstrings
  and long expressions accordingly.
- Secrets only in `iac/.env.dev` (gitignored); `iac/.env.dist` holds
  placeholders.

## Infra gotchas (they recur if forgotten)

- The JMX `-javaagent` is in `KAFKA_OPTS`, so it **leaks into every Kafka CLI**
  run inside a broker container. Broker healthchecks must prefix `KAFKA_OPTS=''`
  or the CLI tries to rebind the metrics port and reports false-unhealthy.
- `init-topics` must pass its script as a YAML **list** (`command:\n  - |`), not
  a scalar `command: |`, otherwise Docker word-splits it and no topics are
  created.
- Broker metrics ports (7071–7073) are **not** published on the host; Prometheus
  scrapes them over the internal network.

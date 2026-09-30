# Data Governance

This platform processes public tweets about Ligue 1 for market research.
Tweets are personal data under GDPR: their authors are identifiable. This
document states what we collect, why, for how long, and who answers for it.

## 1. Roles

| Role | Holder | Responsibility |
|---|---|---|
| Data owner | project lead | purpose, lawful basis, retention decisions |
| Data steward | ingestion maintainer | schema contract, quality SLOs |
| Platform operator | infra maintainer | access control, secrets, retention enforcement |

## 2. Lawful basis and purpose

Purpose: measure volume and engagement of Ligue 1 conversation across four
content themes (stats_analytics, var, mercato, tactique).

Lawful basis: <TO DECIDE — legitimate interest or academic research>.

Out of scope, and never to be introduced without revisiting this document:
profiling of identified individuals, advertising targeting, redistribution
of raw tweets to third parties (also forbidden by the X Developer
Agreement).

## 3. Data dictionary and classification

| Field | Layer | Class | Kept because |
|---|---|---|---|
| `tweet_id` | bronze→silver | pseudonymous | deduplication key, Kafka compaction key |
| `author_id` | bronze→silver | **personal** | unique-author counts, engagement per account |
| `text` | bronze→silver | **personal, free-form** | theme tagging, future sentiment |
| `created_at` | bronze→silver | non-personal | time series |
| `lang` | bronze→silver | non-personal | corpus filter |
| `like_count`, `retweet_count` | bronze→silver | non-personal | engagement metric |
| `hashtags` | bronze→silver | non-personal | theme tagging |
| `themes` | silver | derived | dashboard filter |
| `raw_payload` | bronze | **personal, unminimised** | replay — see §4 |

Classes: *non-personal*, *pseudonymous* (identifies indirectly),
*personal* (identifies directly or nearly so).

## 4. Minimisation

`raw_payload` currently stores the whole API object, including
`entities.urls.expanded_url`, which embeds account handles. It exists to
allow replay from MinIO — a capability we used twice when Kafka retention
had erased the corpus, so it earns its place.

Rule: `raw_payload` lives in bronze (MinIO) only and is stripped before
silver. `user.fields` requests are limited to what the study uses; profile
enrichment (bio, location, profile image) is **not** activated, and
activating it requires updating this document first.

## 5. Retention and erasure

| Store | Current | Target |
|---|---|---|
| Kafka `tweets_raw` | 7 days | unchanged |
| Kafka `tweets_enriched` | compact, **unbounded** | compact + `retention.ms` = 90 days |
| Kafka `audit_logs` | 14 days | unchanged |
| MinIO `tweets-raw` | **no lifecycle rule** | expire objects after 90 days |
| BigQuery staging | **no expiration** | partition expiration 90 days |

Upstream deletions: the X Developer Agreement requires that tweets deleted
on X be removed from our stores. We have no reconciliation job today —
tracked as a known gap, acceptable only while the corpus is a fixed
research snapshot and never republished.

Erasure requests: an author can be erased by deleting their `author_id`
partition-key records from bronze and re-running the replay. This is manual
and untested; write the runbook before any real request.

## 6. Access control

Dev credentials (`minioadmin/minioadmin`, no Kafka SASL, unauthenticated
BigQuery emulator) are **dev-only** and must never reach a shared or
internet-reachable environment. Real values live in `iac/.env.dev`, which
is git-ignored; `iac/.env.dist` carries placeholders only.

Logs are a data path too: `audit_logs` ships ingestion logs containing
tweet text and author ids, and the log file sits on a shared Docker volume.
It inherits the same classification as the data it quotes.

## 7. Quality SLOs

| Indicator | Threshold | Measured by |
|---|---|---|
| theme coverage | ≥ 70 % of collected tweets carry ≥1 theme | offline tally on the corpus |
| validation rate | ≥ 95 % valid | `ExtractionResult.tweets_invalid` |
| duplicate rate | ≤ 10 % per collection window | gold-layer dedup count |
| publishing loss | 0 paid-for tweets unpublished | `backfill.sh` offset guard |

Baseline: coverage went from 0.3 % to 80 % once the ingestion query was
derived from `THEME_KEYWORDS`; duplicates measured at 7 % on the July
backfill.

## 8. Lineage and reproducibility

    X API ──> Kafka tweets_raw ──> MinIO (bronze, date-partitioned)
                    │
                    └─> Spark ──> BigQuery staging (silver)
                              └─> Kafka tweets_enriched (compacted)

Bronze is the replay source of truth: silver can be rebuilt entirely from
MinIO, which is how the corpus was re-tagged twice without re-paying the
API. Any change to `THEME_KEYWORDS` therefore requires a replay, not a new
collection.

## 9. Schema evolution

`iac/dev/storage/bigquery/staging_schema.yaml` is the contract;
`project/pipelines/processing/utils/schema.py` is the reader. All fields are
nullable by design because `from_json` yields nulls on malformed input —
tightening them would let Catalyst prune the null guards in `transform.py`.
New X API fields are ignored unless added to both files in the same commit.

## 10. Cost governance

API spend is $0.005 per tweet, tracked cumulatively in
`api_budget.json` on the shared log volume (`BUDGET_LEDGER_PATH`). An HTTP
402 aborts a run with exit code 2. This exists because a per-process
counter let a restart loop burn ~$6.50 unnoticed.

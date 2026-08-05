# Twitter Market Research — Data Platform (Lakehouse)

Plateforme de données temps réel qui collecte, valide et stocke les tweets X
(Twitter) autour de la **Ligue 1** pour de l'étude de marché (sentiment &
engagement des clubs).

Ce dépôt est **l'entrepôt de données (datawarehouse / lakehouse)** : toute la
chaîne d'ingestion, de stockage et de traitement de la donnée. **Il ne contient
pas de tableaux de bord métier** — uniquement le monitoring technique de la
plateforme.


---

## Flux de données

```
                 ┌─────────────────────────────────────────────┐
   X API v2 ────►│              HybridExtractor                 │
   (payant)      │   (collecte API + enrichissement profils)   │
   twikit  ─────►│                                             │
   (scraping)    └───────────────────┬─────────────────────────┘
                                     │  validation + sérialisation
                                     ▼
                          ┌──────────────────────┐
                          │   Kafka (3 brokers)   │
                          ├──────────────────────┤
       tweets valides ───►│  topic: tweets_raw    │───┐
       tweets rejetés ───►│  topic: audit_logs    │   │
                          └──────────────────────┘   │
                                     │                │
        backup direct (S3RawWriter)  │                │  Kafka Connect
                                     ▼                ▼  (sink S3)
                          ┌──────────────────────────────────────┐
                          │      Object storage (MinIO / S3)      │
                          │        données brutes (source de      │
                          │              vérité)                  │
                          └──────────────────────────────────────┘
                                     │
                                     ▼   (TODO)
                        Processing (Spark) ──► MongoDB (serving)
```

Observabilité : chaque broker expose ses métriques JMX en HTTP (agent
Prometheus), scrapées par **Prometheus**, avec alerting via **AlertManager**.

---

## Stack technique

- **Collecte** : X API v2 (`requests`) + `twikit` (scraping de profils)
- **Streaming** : Apache Kafka (Confluent 7.5, 3 brokers + ZooKeeper)
- **Stockage brut** : MinIO (S3-compatible), alimenté par Kafka Connect (S3 sink)
- **Serving** : MongoDB *(intégration à venir)*
- **Traitement** : Apache Spark *(à venir)*
- **Monitoring** : Prometheus + AlertManager + JMX exporter
- **Sérialisation / validation** : `pydantic`
- **Langage** : Python 3.12
- **Docs** : Sphinx (`docs/`)

---

## Structure du dépôt

```
project/pipelines/
  ingestion/
    tweets_raw/          # Pipeline principale (opérationnelle, testée)
      utils/             # api_extractor, hybrid_extractor, kafka_producer,
                         # key_builder, tweet_validator, tweet_serializer,
                         # s3_writer, scraper_enricher, metrics
      tests/             # Suite pytest (91 tests)
      extract.py         # Point d'entrée CLI de la collecte
    audit_logs/          # Pipeline des rejets (partielle)
  processing/            # Traitement Spark (à venir)
  dashboard/             # (vide — les dashboards vivent dans le dépôt n°3)

iac/dev/                 # Infra de dev (destinée au dépôt IaC)
  kafka/                 # Cluster Kafka + monitoring (Prometheus/AlertManager)
  storage/S3/            # MinIO + Kafka Connect (sink S3)
  storage/mongodb/       # MongoDB
  ingestion/             # Conteneur du producer
  spark/                 # Spark (à venir)

docs/                    # Documentation Sphinx
```

---

## Démarrage rapide

### Prérequis

- Docker + Docker Compose
- Python 3.12
- Un Bearer Token X API v2 (et, optionnellement, un `auth_token` twikit)

### 1. Configurer les secrets

Copier le gabarit et renseigner les vraies valeurs (le fichier réel est
ignoré par git) :

```bash
cp iac/.env.dist iac/.env.dev
# éditer iac/.env.dev : TWITTER_BEARER_TOKEN, TWIKIT_AUTH_TOKEN, ...
```

> ⚠️ Ne jamais committer de secret réel. Les vraies valeurs vont dans
> `iac/.env.dev` (gitignoré), pas dans `iac/.env.dist`.

### 2. Lancer l'infrastructure

```bash
cd iac/dev
docker compose up -d --build
```

Cela démarre : le cluster Kafka (3 brokers + ZooKeeper), l'UI Kafka, MinIO,
Prometheus et AlertManager, et crée les topics (`tweets_raw`,
`tweets_enriched`, `audit_logs`).

### 3. Lancer une collecte

```bash
cd project/pipelines/ingestion/tweets_raw
python extract.py --keywords "Ligue1 OR PSG OR OM" --max-results 250
```

Options utiles : `--no-scrape` (API seule), `--skip-kafka` (mode test),
`--skip-s3`, `--verbose`. Voir `python extract.py --help`.

---

## Interfaces web

| Service | URL | Rôle |
|---------|-----|------|
| Kafka UI | http://localhost:8080 | Explorer topics & messages |
| Prometheus | http://localhost:9090 | Métriques & règles d'alerte (`/targets`, `/rules`) |
| AlertManager | http://localhost:9095 | Alertes actives |
| MinIO (console) | http://localhost:9001 | Console object storage (API S3 sur :9000) |

---

## Tests

La suite utilise un environnement virtuel dédié.

```bash
python -m venv project/.venv
project/.venv/Scripts/python -m pip install -r \
    project/pipelines/ingestion/requirements.txt pytest pytest-cov

# Lancer la suite tweets_raw
project/.venv/Scripts/python -m pytest project/pipelines/ingestion/tweets_raw/tests/
```

État actuel : **91 tests verts** sur la pipeline `tweets_raw`.

---

## Conventions

- **TDD** : test d'abord (rouge → vert).
- **Docstrings NumPy** (`Parameters` / `Returns`) sur les fonctions.
- **Commits** : Conventional Commits (`fix:`, `feat:`, `docs:` …).
- **Secrets** : jamais en dur, toujours via `iac/.env.dev` (gitignoré).

---

## État du projet

| Composant | Statut |
|-----------|--------|
| Ingestion `tweets_raw` (collecte, validation, Kafka, backup S3) | ✅ Opérationnel, testé |
| Cluster Kafka (3 brokers, topics, rétention) | ✅ |
| Sink S3 (Kafka Connect → MinIO) | ✅ Configuré |
| Monitoring (Prometheus / AlertManager / JMX) | ✅ Opérationnel |
| Pipeline `audit_logs` | 🚧 Partielle |
| Intégration MongoDB (serving) | ⬜ À venir |
| Traitement Spark | ⬜ À venir |
| CI/CD (GitLab) | 🚧 Squelette |

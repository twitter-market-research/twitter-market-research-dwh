#!/usr/bin/env bash
# backfill.sh — collecte jour par jour sur l'archive X.
#
# À EXÉCUTER DEPUIS WINDOWS (Git Bash), jamais depuis WSL : la stack
# Kafka/MinIO tourne sur le démon Docker Desktop, alors que WSL utilise
# son propre démon docker-ce. Un conteneur créé côté WSL paierait les
# appels API sans pouvoir publier une seule ligne.
set -uo pipefail

cd "$(dirname "$0")"

KEYWORDS=$(grep '^SEARCH_KEYWORDS=' ../.env.dev | cut -d= -f2-)
[ -n "$KEYWORDS" ] || { echo "SEARCH_KEYWORDS introuvable"; exit 1; }

START="2026-07-06"
DAYS=60
PER_DAY=40

# Somme des offsets du topic : le seul juge de ce qui a réellement atterri.
# Le broker exporte KAFKA_OPTS avec un javaagent JMX ; tout kafka-run-class
# lancé dans le conteneur tenterait de rouvrir le port déjà pris et
# planterait sur BindException. On les neutralise pour l'appel CLI.
# awk sort en erreur si aucune partition n'a été lue, pour ne pas confondre
# un topic vide avec une mesure hors service.
topic_offsets() {
    docker exec -e KAFKA_OPTS= -e JMX_PORT= -e KAFKA_JMX_OPTS= broker-1 \
        kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list broker-1:29092 --topic tweets_raw 2>/dev/null \
        | awk -F: '/^tweets_raw:/ {s += $3; n++} END {if (n) print s; else exit 1}'
}


# Les brokers ont déjà été tués faute de mémoire (exit 137) pendant une
# collecte : 40 tweets facturés, aucun publié. On refuse de dépenser tant
# que les trois brokers ne sont pas sains.
require_healthy_kafka() {
    for b in broker-1 broker-2 broker-3; do
        state=$(docker inspect -f '{{.State.Health.Status}}' "$b" 2>/dev/null)
        if [ "$state" != "healthy" ]; then
            echo "ARRET : $b est '$state' — interrompu avant dépense."
            exit 1
        fi
    done
}

for i in $(seq 0 $((DAYS - 1))); do
    from=$(date -d "$START +$i day" +%Y-%m-%d)
    to=$(date -d "$START +$((i + 1)) day" +%Y-%m-%d)

    require_healthy_kafka
    before=$(topic_offsets) || { echo "ARRET : mesure des offsets HS."; exit 1; }

    echo "=== [$((i + 1))/$DAYS] $from ==="
    docker compose run --rm --no-deps tweets-raw-producer \
        python -m tweets_raw.extract \
        --keywords "$KEYWORDS" \
        --max-results "$PER_DAY" \
        --archive \
        --start-time "${from}T00:00:00Z" \
        --end-time "${to}T00:00:00Z" \
        --log-file /app/logs/ingestion.log

    after=$(topic_offsets) || { echo "ARRET : mesure des offsets HS."; exit 1; }
    echo "--- $from : $((after - before)) messages publiés"
    if [ "$after" -le "$before" ]; then
        echo "ARRET : rien n'a été publié — on ne paie pas 59 fois la même perte."
        exit 1
    fi
    sleep 5
done

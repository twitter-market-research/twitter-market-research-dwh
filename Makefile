# Tasks of the project


ifeq ($(OS),Windows_NT)
PYTHON ?= src/.venv/Scripts/python
else
PYTHON ?= python
endif

REPORTS      := reports
PYTEST_FLAGS := -q
COMPOSE      := docker compose -f iac/dev/docker-compose.yml
ALERT_RULES  := iac/dev/kafka/monitoring/prometheus/alert_rules.yml
IMAGES := tweets-raw-producer logs-producer spark-processing

.PHONY: help install test test-ingestion test-processing governance \
		governance-contract governance-infra check lint up down logs \
		retention-apply clean


# ------- Aide ----------------------------------------------------
help: ## Liste les cibles disponibles
	   @grep -hE '^[a-zA-Z_-]+:.*?## ' ${MAKEFILE_LIST} \
	   | awk 'BEGIN {FS = ":.*?## "} {printf " %-22s %s\n", $$1, $$2}'

.DEFAULT_GOAL := help


# ------- Dependances ---------------------------------------------
install: ## Installe les dépendances des deux pipelines + outils de test
		$(PYTHON) -m pip install --upgrade pip
		$(PYTHON) -m pip install -r src/pipelines/ingestion/requirements.txt
		$(PYTHON) -m pip install -r src/pipelines/processing/requirements.txt
		$(PYTHON) -m pip install pytest pytest-cov pyaml

# -------- Etape test -------------------------------------------
# Toujours lancé depuis la racine : les tests importent src.pipelines...,
# et python -m pytest n'ajoute que le répertoire courant au sys.path.
# Depuis un sous dossier la collecte échoue entièrement.
test: ## Suite complète, avec rapports JUnit et couverture
		@mkdir -p $(REPORTS)/pytest $(REPORTS)/coverage
		${PYTHON} -m pytest src/pipelines ${PYTEST_FLAGS} \
		--junitxml=$(REPORTS)/pytest/junit.xml \
		--cov=src --cov-report=xml:$(REPORTS)/coverage/coverage.xml \
		--cov-report=term-missing

test-ingestion: ## Suite d'ingestion seule (sans JVM)
		$(PYTHON) -m pytest src/pipelines/ingestion $(PYTEST_FLAG)

test-processing: ## Suite processing seule (Spark requis pour tout exécuter)
				 $(PYTHON) -m pytest src/piplines/processing ${PYTEST_FLAG}
# ----- Etape gouvernance -----------------------------------------

governance: governance-contract governance-infra

governance-contract: ## testing all features aof the data-contract
		$(PYTHON) -m pytest \
		  src/pipelines/processing/tests/test_data_contract.py $(PYTEST_FLAGS)

governance-infra: # Infrastructure YAML and Prometheus rules 
		@$(PYTHON) scripts/check_infra_yaml.py
		docker run --rm -v "$(CURDIR):/w"  --entrypoint promtool \
		  prom/prometheus:v2.53.0 check rules /w/$(ALERT_RULES)

# ------ Etape code -------------------------------------------------------
# Non inclus dans 'check' : la base n'a jamais été lintée, un flake8

lint: ## flake8
	-$(PYTHON) -m flake8 --max-line-length 79 --count src/

check: governance test ## Ce que la CI exécute

# ------ Infra locale ---------------------------------------------------
up: ## Démarre la stack de Dev
	$(COMPOSE) up -d

down: ## Stop all the stack
		$(COMPOSE) down

logs: ## Follow logs of Spark job
		$(COMPOSE) logs -f spark-processing



retention-apply: ## Apply declared retentions to existing topics
		  @for t in tweets_raw:604800000 tweets_enriched:7776000000
		  			audit_logs:1209600000; do \
			topic=$${t%:*}; ms=$${t#*:}; \
			docker exec -e KAFKA_OPTS= -e JMX_PORT= broker-1 \
			  kafka-configs --bootstrap-server broker-1:29092 --alter \
			  --entity-type topics --entity-name '$$topic' \
			  --add-config "retention.ms=$$ms"; \
		  done

clean: ## Drop the artifacts of the test
		rm -rf $(REPORTS) .pytest_cache .coverage
		find src -name "__pycache__" -type d-prune -exec rm -rf {} +


# --------------- Etape build ---------------------------------------
# Les trois images applicatives. L'image broker de iac/dev/kafka est
# exclue parce qu'en prod strimzi nous fournira kakfa


build: ## Construit les images applicatives
			$(COMPOSE) build $(IMAGES)

# Vérifier DANS l'image, jamais sur la source. Un backfill a déjà échoué
# sur des drapeaux présents dans le dépot mais absents du conteneur, et
# test ne le voie : seul un import réel dans l'image le révèle.

build-verify: build ## Smoke test des images construites
				$(COMPOSE) run --rm --no deps tweets-raw-producer \
				python -m tweets-raw.extract --help
				$(COMPOSE) run --rm --no-deps --entrypoint python logs-producer \
				-c "import audit_logs.extract; print('audit_logs OK')"
				$(COMPOSE) run --rm --no-deps --entrypoint


# Tasks of the project


ifeq ($(OS),Windows_NT)
PYTHON ?= src/.venv/Scripts/python
else
PYTHON ?= python
endif

AR_REPO     ?= dwh
REGISTRY    ?= europe-west1-docker.pkg.dev/$(GCP_PROJECT_ID)/$(AR_REPO)
VERSION_TAG ?= v$(shell cat VERSION 2>/dev/null)
REPORTS      := reports
PYTEST_FLAGS := -q
COMPOSE      := docker compose -f iac/dev/docker-compose.yml
ALERT_RULES  := iac/dev/kafka/monitoring/prometheus/alert_rules.yml
IMAGES 		 := tweets-raw-producer logs-producer spark-processing
ENV_DEV      := iac/.env.dev
ENV_DIST     := iac/.env.dist

.PHONY: help install test test-ingestion test-processing governance \
		governance-contract governance-infra check lint up down logs \
		retention-apply clean env build build-verify images-push \
		release-check version-bump version flake8 security audit-deps \
		sast scan-secrets


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
		$(PYTHON) -m pip install pytest pytest-cov pyyaml commitizen flake8 bandit pip-audit

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
		$(PYTHON) -m pytest src/pipelines/ingestion $(PYTEST_FLAGS)

test-processing: ## Suite processing seule (Spark requis pour tout exécuter)
				 $(PYTHON) -m pytest src/pipelines/processing ${PYTEST_FLAGS}
# ----- Etape gouvernance -----------------------------------------

governance: governance-contract governance-infra

governance-contract: ## testing all features aof the data-contract
		$(PYTHON) -m pytest \
		  src/pipelines/processing/tests/test_data_contract.py $(PYTEST_FLAGS)

governance-infra: ## Infrastructure YAML and Prometheus rules 
		@$(PYTHON) scripts/check_infra_yaml.py
		docker run --rm -v "$(CURDIR):/w"  --entrypoint promtool \
		  prom/prometheus:v2.53.0 check rules /w/$(ALERT_RULES)

# ------ Etape code -------------------------------------------------------

lint: ## flake8
	$(PYTHON) -m flake8 --max-line-length 79 --count src/

check: governance lint test  ## Ce que la CI exécute

# ------ Infra locale ---------------------------------------------------
up: ## Démarre la stack de Dev
	$(COMPOSE) up -d

down: ## Stop all the stack
		$(COMPOSE) down

logs: ## Follow logs of Spark job
		$(COMPOSE) logs -f spark-processing


retention-apply: ## Apply declared retentions to existing topics
		  @for t in tweets_raw:604800000 tweets_enriched:7776000000 \
		  			audit_logs:1209600000; do \
			topic=$${t%:*}; ms=$${t#*:}; \
			docker exec -e KAFKA_OPTS= -e JMX_PORT= broker-1 \
			  kafka-configs --bootstrap-server broker-1:29092 --alter \
			  --entity-type topics --entity-name "$$topic" \
			  --add-config "retention.ms=$$ms"; \
		  done

clean: ## Drop the artifacts of the test
		rm -rf $(REPORTS) .pytest_cache .coverage
		find src -name "__pycache__" -type d -prune -exec rm -rf {} +

#Les blocs `include` du compose exigent iac/.env.dev, gitignoré car il
# porte les secrets. En CI il n'existe pas : on le dérive du template.
# Les valeurs factices suffisent à CONSTRUIRE les images — les secrets ne
# servent qu'à l'exécution.

$(ENV_DEV):
			cp $(ENV_DIST) $@

env: $(ENV_DEV) ## Crée iac/.env.dev depuis le bon template s'il manque


# --------------- Etape build ---------------------------------------
# Les trois images applicatives. L'image broker de iac/dev/kafka est
# exclue parce qu'en prod strimzi nous fournira kakfa


build: $(ENV_DEV) ## Construit les images applicatives
			$(COMPOSE) build $(IMAGES)

# Vérifier DANS l'image, jamais sur la source. Un backfill a déjà échoué
# sur des drapeaux présents dans le dépot mais absents du conteneur, et
# test ne le voie : seul un import réel dans l'image le révèle.

build-verify: build ## Smoke test des images construites
				$(COMPOSE) run --rm --no-deps tweets-raw-producer \
				python -m tweets_raw.extract --help
				$(COMPOSE) run --rm --no-deps --entrypoint python logs-producer \
				-c "import audit_logs.extract; print('audit_logs OK')"
				$(COMPOSE) run --rm --no-deps --entrypoint python3 spark-processing \
				-c "import src.pipelines.processing.stream; print('stream OK')"

# Deploiment par version explicite : aucun tag 'latest' n'est poussé.
# le registre est en immutable tags, donc un push d'une version déjà présente échoue
images-push: build ## Pousse les trois images sous leur tag de version
				@test -n "$(GCP_PROJECT_ID)" || \
				  { echo "GCP_PROJECT_ID requis"; exit 1; }
				@for img in ${IMAGES}; do \
				  docker tag twitter-market-research-$$img:latest \
				  	${REGISTRY}/$$img:${VERSION_TAG}; \
				  docker push ${REGISTRY}/$$img:${VERSION_TAG}; \
				done


# --------------- Etape sécurité ---------------------------------------
security: audit-deps sast scan-secrets  ## Audit deps + SAST + secrets

audit-deps: ## CVE connues des dependances declarees
		$(PYTHON) -m pip_audit -r src/pipelines/ingestion/requirements.txt
		$(PYTHON) -m pip_audit -r src/pipelines/processing/requirements.txt

sast: ## Analyse statique
		$(PYTHON) -m bandit -r src/pipelines -ll -x "*/tests/*"

# --redact : sans lui gitleaks recopie le secret trouve en clair dans les
# logs CI, conserves 90 jours. Les trois GIT_CONFIG_* desarment le refus
# "dubious ownership" de git : le conteneur tourne en root alors que le
# checkout appartient a l'uid du runner, et gitleaks appelle git log.
scan-secrets: ## gitleaks sur tout l'historique git
		docker run --rm -v "$(CURDIR):/repo" \
			-e GIT_CONFIG_COUNT=1 \
			-e GIT_CONFIG_KEY_0=safe.directory \
			-e GIT_CONFIG_VALUE_0=/repo \
			zricethezav/gitleaks:v8.18.4 detect --source=/repo \
			  --no-banner --redact


# --------------------- Etape release -----------------------------

release-check: ## Valide les messages de commit de la branche
				${PYTHON} -m commitizen check --rev-range origin/main..HEAD

# commitizen sort en code 21 (NoneIncrementExit) quand aucun commit ne
# justifie d'increment. Ce n'est pas une erreur : la traiter comme telle
# ferait rougir la CI sur un push qui ne contient que des `docs:`.
version-bump: ## Calcule la version; tague et met à jour le CHANGELOG
				@$(PYTHON) -m commitizen bump --yes --changelog; \
				code=$$?; \
				if [ $$code -eq 21 ]; then \
					echo "Aucun commit ne justifie en bump - rien à faire."; \
				elif [ $$code -ne 0 ]; then \
					exit $$code; \
				fi

version: ## Affiche la version courante
				@cat VERSION


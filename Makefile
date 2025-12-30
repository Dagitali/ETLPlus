# Makefile
# ETLPlus
#
# Copyright © 2025 Dagitali LLC. All rights reserved.
#
# Facilitates automation for setting up Unix-based systems.
#
# See:
# 1. https://earthly.dev/blog/python-makefile/
# 2. https://www.gnu.org/prep/standards/html_node/Makefile-Conventions.html
# 3. https://www.gnu.org/software/make
# 4. https://www.gnu.org/software/make/manual/html_node/Include.html
# 5. https://www.gnu.org/software/make/manual/make.html
# 6. https://blog.mathieu-leplatre.info/tips-for-your-makefile-with-python.html
# 7. https://medium.com/aigent/makefiles-for-python-and-beyond-5cf28349bf05
# 8. https://web.mit.edu/gnu/doc/html/make_1.html
# 9. https://ricardoanderegg.com/posts/makefile-python-project-tricks/
# 10. https://stackoverflow.com/questions/24736146/how-to-use-virtualenv-in-makefile
# 11. https://venthur.de/2021-03-31-python-makefiles.html
#
# Common flows:
#
# 1) Create venv + install dev tooling and the package (editable).
# $ make dev
#
# 2) Run static checks & tests.
# $ make check
#
# 3) Try the CLI in dry-run (uses etlplus/.env by default).
# $ make run
#
# 4) Actually post to WorkMax (be careful).
# $ make run-now
#
# 5) Clean build artifacts or nuke the venv.
# $ make clean
# $ make clean-venv


# SECTION: INCLUDES ========================================================= #


-include .env


# SECTION: VARIABLES ======================================================== #


### Make ###

.DEFAULT_GOAL := help

### Docker ###

DOCKER_BACKUPS_DIR ?= ./backups
DOCKER_COMPOSE ?= docker-compose.yml
DOCKER_CONTAINER ?= $(PG_CONTAINER_NAME)

### Docker Compose ###

COMPOSE_PROJECT_NAME ?= etlplus

COMPOSE_FLAGS ?= -f $(DOCKER_COMPOSE) -p $(COMPOSE_PROJECT_NAME)
COMPOSE_PROFILES ?= ui
COMPOSE_SERVICE ?= postgres
COMPOSE_WEB_SERVICE ?= adminer

# Optional: override the env-file Compose uses for variable substitution.
# Leave blank to use default .env semantics.
COMPOSE_ENV ?=
COMPOSE_ENV_OPT := $(if $(COMPOSE_ENV),--env-file $(COMPOSE_ENV),)

# Unified docker compose command (DRY)
COMPOSE := docker compose $(COMPOSE_ENV_OPT) $(COMPOSE_FLAGS)

WEB_PORT_HOST ?= 8080

### Python ###

# Python to bootstrap the venv (override on the CLI: make venv PY=python3.13)
PY ?= python3

# Package root (where pyproject.toml lives)
# PKG_DIR := etlplus
PKG_DIR := .

# Virtualenv lives inside the package folder
VENV_DIR := $(PKG_DIR)/.venv

# Cross-platform venv bin paths
ifeq ($(OS),Windows_NT)
	VENV_BIN := $(VENV_DIR)/Scripts
	PYTHON   := $(VENV_BIN)/python.exe
	PIP      := $(VENV_BIN)/pip.exe
	ETLPLUS  := $(VENV_BIN)/etlplus.exe
else
	VENV_BIN := $(VENV_DIR)/bin
	PYTHON   := $(VENV_BIN)/python
	PIP      := $(VENV_BIN)/pip
	ETLPLUS  := $(VENV_BIN)/etlplus
endif

### SDLC ###

# Default .env used by run targets (override: make run ENV=.env.local)
ENV ?= $(PKG_DIR)/.env


# SECTION: MACROS =========================================================== #


define ECHO_OK
	printf "\033[32m✔\033[0m %s\n" "$(1)"
endef
define ECHO_INFO
	printf "\033[36mℹ\033[0m %s\n" "$(1)"
endef


# SECTION: PHONY TARGETS ==================================================== #


##@ Utilities

.PHONY: bootstrap
bootstrap: ## Create .env.postgres and standard dirs (one-time)
	@mkdir -p $(DOCKER_BACKUPS_DIR)
	@mkdir -p docker/postgres/initdb
	@if [ ! -f .env.postgres ]; then \
		echo "Creating .env.postgres"; \
		printf "POSTGRES_USER=postgres\n" > .env.postgres; \
		printf "POSTGRES_PASSWORD=postgres\n" >> .env.postgres; \
		printf "POSTGRES_DB=etlplus\n" >> .env.postgres; \
		printf "PG_PORT=5432\n" >> .env.postgres; \
		printf "PG_CONTAINER_NAME=postgres\n" >> .env.postgres; \
		printf "PGADMIN_PORT=8080\n" >> .env.postgres; \
	else \
		echo ".env.postgres already exists; skipping"; \
	fi
	@$(call ECHO_OK,"Bootstrap complete")

.PHONY: check
check: doclint lint typecheck test ## Run docstring lint, code lint, type-check, and tests

.PHONY: clean
clean: ## Remove build artifacts and caches
	@find . -name '__pycache__' -type d -prune -exec rm -rf {} + 2>/dev/null || true
	@find . -name '.pytest_cache' -type d -prune -exec rm -rf {} + 2>/dev/null || true
	@rm -rf $(PKG_DIR)/build $(PKG_DIR)/dist $(PKG_DIR)/.mypy_cache 2>/dev/null || true
	@rm -rf $(PKG_DIR)/src/*.egg-info 2>/dev/null || true
	@$(call ECHO_OK,"Cleaned artifacts")

.PHONY: clean-venv
clean-venv: ## Remove the virtual environment
	@rm -rf "$(VENV_DIR)"
	@$(call ECHO_OK,"Removed venv")

.PHONY: demo-snippets
demo-snippets: ## Rebuild recorded CLI snippets embedded in DEMO.md
	@$(PYTHON) tools/update_demo_snippets.py
	@$(call ECHO_OK,"Refreshed demo snippets")

.PHONY: dev
dev: venv ## Install package + dev tools (pytest, ruff, mypy, etc.)
	@$(PYTHON) -m pip install -e $(PKG_DIR)[dev]
	@$(call ECHO_OK,"Installed etlplus + dev extras")

.PHONY: dist
dist: ## Build sdist and wheel into ./dist using pyproject.toml
	@$(PYTHON) -m pip install --upgrade build twine >/dev/null
	@$(PYTHON) -m build
	@$(PYTHON) -m twine check dist/*
	@$(call ECHO_OK,"Built and validated distribution artifacts in ./dist")

.PHONY: fix
fix: ## Auto-fix with ruff
	@$(VENV_BIN)/ruff check . --fix || (echo "Hint: run 'make dev' first" && false)

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS=":.*##"; printf "\nUsage: make \033[36m<TARGET>\033[0m\n\nTargets:\n"} \
	/^[a-zA-Z0-9_\-]+:.*##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } \
	/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0,5) } ' $(MAKEFILE_LIST)

.PHONY: install
install: venv ## Install the package (editable) and runtime deps
	@$(PYTHON) -m pip install -e $(PKG_DIR)
	@$(call ECHO_OK,"Installed etlplus (editable)")

.PHONY: lint
lint: ## Run ruff lint checks
	@$(PYTHON) -c "import sys; sys.exit(0)" >/dev/null 2>&1 || true
	@$(ETLPLUS) --help >/dev/null 2>&1 || true
	@$(VENV_BIN)/ruff check . || (echo "Hint: run 'make dev' first" && false)

.PHONY: doclint
doclint: ## Run docstring linters (pydocstyle + pydoclint if available)
	@echo "Running pydocstyle (NumPy convention)…"; \
	if [ -x "$(VENV_BIN)/pydocstyle" ]; then \
	$(VENV_BIN)/pydocstyle etlplus; \
	else \
	echo "pydocstyle not installed; install with 'make dev'"; \
	fi
	@echo "Running pydoclint (NumPy style)…"; \
	if [ -x "$(VENV_BIN)/pydoclint" ]; then \
	$(VENV_BIN)/pydoclint --style numpy etlplus; \
	else \
	echo "pydoclint not installed; skip (optional)"; \
	fi

.PHONY: fmt
fmt: ## Format code with ruff (imports + fixes) and black
	@$(VENV_BIN)/ruff check . --fix || (echo "Hint: run 'make dev' first" && false)
	@$(VENV_BIN)/ruff format . || true
	@$(VENV_BIN)/black . || true

.PHONY: run
run: ## Run the etlplus CLI (dry-run) using $(ENV)
	@$(ETLPLUS) --env "$(ENV)" --since 2025-09-01 --dry-run

.PHONY: run-now
run-now: ## Run the etlplus CLI without dry-run (CAUTION: posts to WorkMax)
	@$(ETLPLUS) --env "$(ENV)"

.PHONY: show-venv
show-venv: ## Print venv and interpreter locations
	@echo "VENV_DIR   = $(VENV_DIR)"
	@echo "VENV_BIN   = $(VENV_BIN)"
	@echo "PYTHON     = $(PYTHON)"
	@echo "PIP        = $(PIP)"
	@echo "ETLPLUS    = $(ETLPLUS)"

.PHONY: test
test: ## Run tests with pytest
	@PYTHONPATH=. $(VENV_BIN)/pytest || (echo "Hint: run 'make dev' first" && false)

.PHONY: typecheck
typecheck: ## Type-check with mypy
	@$(VENV_BIN)/mypy $(PKG_DIR)/src/etlplus || (echo "Hint: run 'make dev' first" && false)

.PHONY: up
up: ## First-time setup: bootstrap, pull images, start DB+UI, wait healthy
	@$(MAKE) bootstrap
	@$(MAKE) compose-pull
	@$(MAKE) compose-up
	@$(MAKE) compose-wait-db

.PHONY: venv
venv: ## Create the virtual environment (at $(VENV_DIR))
	@if [ ! -d "$(VENV_DIR)" ]; then \
		$(call ECHO_INFO, "Creating venv with $(PY) → $(VENV_DIR)"); \
		$(PY) -m venv .venv; \
	else \
		$(call ECHO_INFO, "Using existing venv: $(VENV_DIR)"); \
	fi
	@$(PYTHON) -m pip install --upgrade pip setuptools wheel >/dev/null
	@$(call ECHO_OK,"venv ready")

##@ CI

.PHONY: ci-smoke
ci-smoke: ## Boot DB, wait healthy, run 'select 1', then stop (keep volumes)
	@$(MAKE) compose-up
	@$(MAKE) compose-wait-db
	@$(MAKE) db-run-sql Q="select 1"
	@$(MAKE) compose-stop

##@ Docker

.PHONY: docker-clean
docker-clean: ## Stop stack and remove images/volumes/orphans
	@echo "Cleaning up Compose stack, images, volumes, and orphans..."
	@$(COMPOSE) down --rmi all --volumes --remove-orphans

.PHONY: docker-nuke
docker-nuke: ## Stop and remove EVERYTHING (images, volumes, networks)
	@echo "NUKING: containers, images, volumes, networks (be careful)…"
	@$(COMPOSE) down --rmi all --volumes --remove-orphans
	@docker volume prune -f
	@docker network prune -f

##@ Docker Compose

.PHONY: compose-config
compose-config: ## Render resolved compose config
	@echo "Rendering resolved compose config..."
	@$(COMPOSE) config

.PHONY: compose-down
compose-down: compose-stop ## Stop stack (keep volumes/images)
	@:

.PHONY: compose-logs
compose-logs: ## Tail logs (S=name|all; default: $(COMPOSE_SERVICE); TAIL_SINCE=1h ok)
	@SVC="$(S)"; SINCE_OPT=""; \
	if [ -n "$(TAIL_SINCE)" ]; then SINCE_OPT="--since $(TAIL_SINCE)"; fi; \
	if [ -z "$$SVC" ]; then SVC="$(COMPOSE_SERVICE)"; fi; \
	if [ "$$SVC" = "all" ]; then \
		echo "Tailing logs for all services..."; \
		$(COMPOSE) logs $$SINCE_OPT -f; \
	else \
		echo "Tailing logs for service '$$SVC'..."; \
		$(COMPOSE) logs $$SINCE_OPT -f $$SVC; \
	fi

.PHONY: compose-pull
compose-pull: ## Pull images defined in docker-compose.yml
	@echo "Pulling images defined in $(DOCKER_COMPOSE)..."
	@$(COMPOSE) pull

.PHONY: compose-restart
compose-restart: ## Restart a service (S=name; default: $(COMPOSE_SERVICE))
	@SVC="$(S)"; if [ -z "$$SVC" ]; then SVC="$(COMPOSE_SERVICE)"; fi; \
	echo "Restarting service '$$SVC'..."; \
	$(COMPOSE) restart $$SVC

.PHONY: compose-restart-db
compose-restart-db:
	@$(MAKE) compose-restart S=$(COMPOSE_SERVICE)

.PHONY: compose-restart-ui
compose-restart-ui:
	@$(MAKE) compose-restart S=$(COMPOSE_WEB_SERVICE)

.PHONY: compose-status
compose-status: ## Show status (S=name; default: all, or service if set)
	@SVC="$(S)"; \
	if [ -z "$$SVC" ] || [ "$$SVC" = "all" ]; then \
		$(COMPOSE) ps; \
	else \
		$(COMPOSE) ps $$SVC; \
	fi

.PHONY: compose-stop
compose-stop: ## Stop stack (keep volumes/images)
	@echo "Stopping services (keeping volumes and images)…"
	@$(COMPOSE) down --remove-orphans


.PHONY: compose-up
compose-up: compose-up-ui ## Start DB + UI and open browser
	@:

.PHONY: compose-up-all
compose-up-all: compose-up ## (compat) Use 'make compose-up' instead
	@:

.PHONY: compose-up-ui
compose-up-ui: ## Start Adminer UI (profile: ui) and open browser
	@echo "Starting Adminer UI (profile: ui)…"
	@COMPOSE_PROFILES=$(COMPOSE_PROFILES) $(COMPOSE) up -d --build $(COMPOSE_WEB_SERVICE)
	@$(MAKE) ui-url
	@$(MAKE) ui-open

.PHONY: compose-wait-db
compose-wait-db: ## Wait until Postgres service is healthy
	@CID=$$($(COMPOSE) ps -q $(COMPOSE_SERVICE)); \
	if [ -z "$$CID" ]; then \
		echo "Service '$(COMPOSE_SERVICE)' not running. Start it with 'make compose-up'."; \
		exit 1; \
	fi; \
	echo "Waiting for '$(COMPOSE_SERVICE)' to be healthy…"; \
	for i in $$(seq 1 60); do \
	    STATUS=$$(docker inspect -f '{{json .State.Health.Status}}' $$CID 2>/dev/null | tr -d '"'); \
	    if [ "$$STATUS" = "healthy" ]; then echo "Service is healthy."; exit 0; fi; \
	    sleep 1; \
	done; \
	echo "Timed out waiting for health."; exit 1

##@ Postgres

.PHONY: db-backup
db-backup: ## Dump DB to ./backups as timestamped .sql.gz
	@mkdir -p $(DOCKER_BACKUPS_DIR)
	@$(COMPOSE) exec -T $(COMPOSE_SERVICE) \
		sh -lc 'PGPASSWORD="$$POSTGRES_PASSWORD" \
		pg_dump --clean --if-exists --no-owner --no-privileges \
		-U "$$POSTGRES_USER" "$$POSTGRES_DB"' \
		| gzip -c > $(DOCKER_BACKUPS_DIR)/$$(date +"%Y%m%d_%H%M%S").sql.gz
	@echo "Wrote dump to $(DOCKER_BACKUPS_DIR)"

.PHONY: db-restore
db-restore: ## Restore DB from F=backups/file.sql[.gz]
	@[ -n "$(F)" ] || (echo 'Usage: make db-restore F=backups/file.sql[.gz]' && exit 1)
	@EXT=$$(printf "%s" "$(F)" | awk -F. '{print $$NF}'); \
	if [ "$$EXT" = "gz" ]; then CAT="gunzip -c"; else CAT="cat"; fi; \
	$$CAT "$(F)" | $(COMPOSE) exec -T $(COMPOSE_SERVICE) \
		sh -lc 'PGPASSWORD="$$POSTGRES_PASSWORD" \
		psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

.PHONY: db-migrate
db-migrate: ## Apply all SQL files in migrations/ (alphabetical)
	@sh -c 'set -e; \
	if ! ls migrations/*.sql >/dev/null 2>&1; then \
		echo "No migrations/*.sql found"; exit 1; \
	fi; \
	for f in migrations/*.sql; do \
		echo "Applying $$f"; \
		$(COMPOSE) exec -T $(COMPOSE_SERVICE) sh -lc \
		"PGPASSWORD=\"\$$POSTGRES_PASSWORD\" psql -v ON_ERROR_STOP=1 -U \"\$$POSTGRES_USER\" -d \"\$$POSTGRES_DB\"" < "$$f"; \
	done'

.PHONY: db-run-sql
db-run-sql: ## Run inline SQL (Q="...") or file (F=...) against DB
	@SQL_FLAG=""; \
	if [ -n "$(Q)" ]; then SQL_FLAG="-c \"$(Q)\""; fi; \
	if [ -n "$(F)" ]; then \
		EXT=$$(printf "%s" "$(F)" | awk -F. '{print $$NF}'); \
		if [ "$$EXT" = "gz" ]; then CAT="gunzip -c"; else CAT="cat"; fi; \
		$$CAT "$(F)" | $(COMPOSE) exec -T $(COMPOSE_SERVICE) \
			sh -lc 'PGPASSWORD="$$POSTGRES_PASSWORD" \
			psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'; \
		else \
			$(COMPOSE) exec -T $(COMPOSE_SERVICE) \
			sh -lc "PGPASSWORD=\"\$$POSTGRES_PASSWORD\" psql -v ON_ERROR_STOP=1 -U \"\$$POSTGRES_USER\" -d \"\$$POSTGRES_DB\" $$SQL_FLAG"; \
		fi

.PHONY: db-shell
db-shell: ## Open shell in Postgres container
	@$(COMPOSE) exec -it $(COMPOSE_SERVICE) sh

.PHONY: db-url
db-url:
	@echo "postgresql://$${POSTGRES_USER:-postgres}:$${POSTGRES_PASSWORD:-postgres}@localhost:$${PG_PORT:-5432}/$${POSTGRES_DB:-postgres}"

.PHONY: psql
psql: ## Open psql shell (Q="...": run query first)
	@CID=$$($(COMPOSE) ps -q $(COMPOSE_SERVICE)); \
	if [ -z "$$CID" ]; then \
		echo "Service '$(COMPOSE_SERVICE)' not running."; \
		echo "Run 'make compose-up'."; \
		exit 1; \
	fi; \
	if [ -n "$(Q)" ]; then \
		$(MAKE) db-run-sql Q="$(Q)"; \
	fi; \
	$(COMPOSE) exec -it $(COMPOSE_SERVICE) \
		sh -lc 'PGPASSWORD="$$POSTGRES_PASSWORD" \
		psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

##@ UI

.PHONY: ui-open
ui-open: ## Open Adminer in your default browser
	@URL="http://localhost:$(WEB_PORT_HOST)"; \
	if command -v open >/dev/null 2>&1; then open $$URL; \
	elif command -v xdg-open >/dev/null 2>&1; then xdg-open $$URL; \
	else echo "Open $$URL in your browser."; fi

.PHONY: ui-url
ui-url: ## Print Adminer URL
	@echo "Adminer UI → http://localhost:$(WEB_PORT_HOST)"

##@ Pipeline

.PHONY: pipeline-run
pipeline-run: ## Run a job from in/pipeline.yml (JOB=name, CONFIG=path)
	@[ -n "$(JOB)" ] || (echo "Usage: make pipeline-run JOB=<name> [CONFIG=in/pipeline.yml]" && exit 1)
	@CFG=$(if $(CONFIG),$(CONFIG),in/pipeline.yml); \
		echo "Running pipeline job '$(JOB)' with $$CFG"; \
		$(PYTHON) tools/run_pipeline.py --config "$$CFG" --job "$(JOB)"

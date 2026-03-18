# ── PySpark DataFrame — Docker Makefile ───────────────────────────────────────
# Run from: infra/docker/
#
# Usage:
#   make build          Build app + jupyter images
#   make up             Start full stack (master, worker, mysql, app, jupyter)
#   make down           Stop and remove all containers
#   make run S=<path>   Run a single script inside pyspark-app
#   make test           Run pytest inside pyspark-app
#   make shell          Open an interactive PySpark shell
#   make bash           Open a bash shell in pyspark-app
#   make jupyter        Start only the JupyterLab service
#   make mysql          Start only MySQL
#   make logs           Tail all container logs
#   make clean          Stop everything + remove volumes
#   make ps             Show running containers
# ──────────────────────────────────────────────────────────────────────────────

COMPOSE := docker compose
PROJECT := pyspark-df

# Load .env if present
ifneq (,$(wildcard .env))
  include .env
  export
endif

.PHONY: build up down run test submit shell bash jupyter mysql logs clean ps help

# ── Images ────────────────────────────────────────────────────────────────────

build:
	$(COMPOSE) -p $(PROJECT) build

build-app:
	$(COMPOSE) -p $(PROJECT) build pyspark-app

build-jupyter:
	$(COMPOSE) -p $(PROJECT) build jupyter

# ── Stack lifecycle ───────────────────────────────────────────────────────────

up:
	$(COMPOSE) -p $(PROJECT) up -d

down:
	$(COMPOSE) -p $(PROJECT) down

clean:
	$(COMPOSE) -p $(PROJECT) down -v --remove-orphans

ps:
	$(COMPOSE) -p $(PROJECT) ps

logs:
	$(COMPOSE) -p $(PROJECT) logs -f --tail=100

# ── Run scripts ───────────────────────────────────────────────────────────────
# Usage: make run S=src/data_frame/etl/etl.py

run:
ifndef S
	@echo "Usage: make run S=<script.py>"
	@echo "  e.g. make run S=src/data_frame/etl/etl.py"
	@exit 1
endif
	$(COMPOSE) -p $(PROJECT) run --rm pyspark-app run $(S)

submit:
ifndef S
	@echo "Usage: make submit S=<script.py>"
	@exit 1
endif
	$(COMPOSE) -p $(PROJECT) run --rm pyspark-app submit $(S)

# ── Testing ───────────────────────────────────────────────────────────────────
# Usage: make test  OR  make test T=tests/data_frame/creation/ -v

test:
	$(COMPOSE) -p $(PROJECT) run --rm pyspark-app test $(T)

# ── Interactive ───────────────────────────────────────────────────────────────

shell:
	$(COMPOSE) -p $(PROJECT) run --rm pyspark-app shell

bash:
	$(COMPOSE) -p $(PROJECT) run --rm pyspark-app bash

# ── Individual services ──────────────────────────────────────────────────────

jupyter:
	$(COMPOSE) -p $(PROJECT) up -d jupyter

mysql:
	$(COMPOSE) -p $(PROJECT) up -d mysql

spark-cluster:
	$(COMPOSE) -p $(PROJECT) up -d spark-master spark-worker

# ── Scale workers ─────────────────────────────────────────────────────────────
# Usage: make scale N=3

scale:
ifndef N
	@echo "Usage: make scale N=<workers>"
	@exit 1
endif
	$(COMPOSE) -p $(PROJECT) up -d --scale spark-worker=$(N)

# ── Help ──────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  make build                Build app + jupyter images"
	@echo "  make up                   Start full stack"
	@echo "  make down                 Stop all containers"
	@echo "  make run S=<script>       Run a Python script in the app container"
	@echo "  make submit S=<script>    spark-submit a script"
	@echo "  make test [T=<path>]      Run pytest (optionally scoped)"
	@echo "  make shell                Interactive PySpark REPL"
	@echo "  make bash                 Bash shell in app container"
	@echo "  make jupyter              Start JupyterLab only"
	@echo "  make mysql                Start MySQL only"
	@echo "  make spark-cluster        Start Spark master + worker"
	@echo "  make scale N=<workers>    Scale Spark workers"
	@echo "  make logs                 Tail container logs"
	@echo "  make clean                Stop + remove volumes"
	@echo "  make ps                   Show running containers"
	@echo ""

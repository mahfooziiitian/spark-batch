.PHONY: up down build ps logs clean submit scale

COMPOSE = docker compose
WORKERS ?= 2

## Start the full cluster (master + workers + history server + notebook)
up:
	$(COMPOSE) up -d

## Build the notebook image
build:
	$(COMPOSE) build notebook

## Stop and remove containers (keeps volumes)
down:
	$(COMPOSE) down

## Show running services
ps:
	$(COMPOSE) ps

## Stream logs (Ctrl-C to exit)
logs:
	$(COMPOSE) logs -f

## Stream logs for a single service: make logs-master
logs-%:
	$(COMPOSE) logs -f $*

## Scale workers: make scale WORKERS=4
scale:
	$(COMPOSE) up -d --scale spark-worker=$(WORKERS)

## Submit the default ETL job
submit:
	$(COMPOSE) --profile submit run --rm spark-submit

## Submit a custom job file: make submit-job JOB_FILE=jobs/etl_job.py
submit-job:
	JOB_FILE=$(JOB_FILE) $(COMPOSE) --profile submit run --rm spark-submit

## Open the Spark Master UI (macOS/Linux)
ui-master:
	open http://localhost:8080 2>/dev/null || xdg-open http://localhost:8080

## Open the History Server UI
ui-history:
	open http://localhost:18080 2>/dev/null || xdg-open http://localhost:18080

## Open JupyterLab
ui-notebook:
	open http://localhost:8888 2>/dev/null || xdg-open http://localhost:8888

## Stop containers and remove volumes (full reset)
clean:
	$(COMPOSE) down -v --remove-orphans

# pyspark-ds-custom — Development Automation
# Usage: make <target>

.PHONY: help install install-spark install-databricks lint format type-check security gitleaks test test-verbose \
        test-cov docs docs-serve clean all ci \
        bundle-validate bundle-deploy bundle-deploy-prod bundle-destroy \
        bundle-run-basic bundle-run-restapi bundle-run-community bundle-run-uc-auth \
        bundle-run-all bundle-summary bundle-logs

PYTHON := uv run python
PYTEST := uv run pytest
RUFF   := uv run ruff
BANDIT := uv run bandit

# Default target
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Setup ────────────────────────────────────────────────────────────────────

install: ## Install dev deps (no PySpark — use install-spark or install-databricks)
	uv sync

install-spark: ## Install with PySpark extra (local mode)
	uv sync --extra spark

install-databricks: ## Install with Databricks Connect + SDK (remote mode)
	uv sync --extra databricks

# ─── Code Quality ─────────────────────────────────────────────────────────────

lint: ## Run ruff linter (check mode)
	$(RUFF) check src/ tests/ examples/

format: ## Format code with ruff
	$(RUFF) format src/ tests/ examples/

format-check: ## Check formatting without modifying files
	$(RUFF) format --check src/ tests/ examples/

type-check: ## Run type checking with pyright
	uv run pyright src/

# ─── Security ─────────────────────────────────────────────────────────────────

security: ## Run bandit security scanner
	$(BANDIT) -r src/ -c pyproject.toml -q

gitleaks: ## Scan for secrets/credentials with gitleaks
	gitleaks detect --source . --no-banner -v

gitleaks-protect: ## Pre-commit gitleaks check (staged files only)
	gitleaks protect --source . --no-banner --staged -v

# ─── Testing ──────────────────────────────────────────────────────────────────

test: ## Run tests — use MARK=pyspark|databricks to filter (default: all)
	$(PYTEST) $(if $(MARK),-m $(MARK),)

test-verbose: ## Run tests (verbose output)
	$(PYTEST) -v --tb=short $(if $(MARK),-m $(MARK),)

test-cov: ## Run tests with coverage report
	$(PYTEST) --cov=custom_ds --cov-report=term-missing --cov-report=html:htmlcov $(if $(MARK),-m $(MARK),)

test-watch: ## Run tests in watch mode (requires pytest-watch)
	uv run ptw -- --tb=short

# ─── Documentation ────────────────────────────────────────────────────────────

docs: ## Build MkDocs documentation
	uv run mkdocs build --strict

docs-serve: ## Serve docs locally with live-reload
	uv run mkdocs serve --dev-addr 127.0.0.1:8000 --livereload

docs-deploy: ## Deploy docs to GitHub Pages
	uv run mkdocs gh-deploy --force

# ─── Maintenance ──────────────────────────────────────────────────────────────

clean: ## Remove build artifacts
	rm -rf site/ htmlcov/ .pytest_cache/ .ruff_cache/ dist/ build/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

# ─── Composite Targets ────────────────────────────────────────────────────────

check: lint format-check security gitleaks ## Run all code quality checks

ci: install install-spark lint format-check security test-cov docs ## Full CI pipeline

all: install check test docs ## Install + check + test + docs

# ─── Databricks Asset Bundle ──────────────────────────────────────────────────

TARGET ?= dev
PROFILE ?=
PROFILE_FLAG := $(if $(PROFILE),--profile $(PROFILE),)

bundle-validate: ## Validate DABs configuration (TARGET=dev|staging|prod PROFILE=<name>)
	databricks bundle validate --strict -t $(TARGET) $(PROFILE_FLAG)

bundle-deploy: ## Deploy DABs to workspace (TARGET=dev|staging|prod PROFILE=<name>)
	databricks bundle deploy -t $(TARGET) $(PROFILE_FLAG)

bundle-deploy-prod: ## Deploy DABs to production (with auto-approve)
	databricks bundle deploy -t prod --auto-approve $(PROFILE_FLAG)

bundle-summary: ## Show deployed bundle summary (TARGET=dev|staging|prod PROFILE=<name>)
	databricks bundle summary -t $(TARGET) $(PROFILE_FLAG)

bundle-run-basic: ## Run basic examples job (TARGET=dev|staging|prod PROFILE=<name>)
	databricks bundle run basic_examples -t $(TARGET) $(PROFILE_FLAG)

bundle-run-restapi: ## Run REST API examples job (TARGET=dev|staging|prod PROFILE=<name>)
	databricks bundle run restapi_examples -t $(TARGET) $(PROFILE_FLAG)

bundle-run-community: ## Run community sources job (TARGET=dev|staging|prod PROFILE=<name>)
	databricks bundle run community_examples -t $(TARGET) $(PROFILE_FLAG)

bundle-run-uc-auth: ## Run UC HTTP auth example (TARGET=dev|staging|prod PROFILE=<name>)
	databricks bundle run uc_http_auth_example -t $(TARGET) $(PROFILE_FLAG)

bundle-run-all: bundle-run-basic bundle-run-restapi bundle-run-community bundle-run-uc-auth ## Run all DABs jobs

bundle-destroy: ## Destroy deployed DABs resources (TARGET=dev|staging|prod PROFILE=<name>)
	databricks bundle destroy -t $(TARGET) $(PROFILE_FLAG)

bundle-ci: bundle-validate bundle-deploy ## Validate + deploy bundle (TARGET=dev|staging|prod PROFILE=<name>)

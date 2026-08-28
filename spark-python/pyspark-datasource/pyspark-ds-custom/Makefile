# pyspark-ds-custom — Development Automation
# Usage: make <target>

.PHONY: help install lint format type-check security gitleaks test test-verbose \
        test-cov docs docs-serve clean all ci

PYTHON := uv run python
PYTEST := uv run pytest
RUFF   := uv run ruff
BANDIT := uv run bandit

# Default target
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Setup ────────────────────────────────────────────────────────────────────

install: ## Install all dependencies (runtime + dev)
	uv sync

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

test: ## Run tests (quiet)
	$(PYTEST)

test-verbose: ## Run tests (verbose output)
	$(PYTEST) -v --tb=short

test-cov: ## Run tests with coverage report
	$(PYTEST) --cov=custom_ds --cov-report=term-missing --cov-report=html:htmlcov

test-watch: ## Run tests in watch mode (requires pytest-watch)
	uv run ptw -- --tb=short

# ─── Documentation ────────────────────────────────────────────────────────────

docs: ## Build MkDocs documentation
	uv run mkdocs build --strict

docs-serve: ## Serve docs locally with live-reload
	uv run mkdocs serve --dev-addr 127.0.0.1:8000

docs-deploy: ## Deploy docs to GitHub Pages
	uv run mkdocs gh-deploy --force

# ─── Maintenance ──────────────────────────────────────────────────────────────

clean: ## Remove build artifacts
	rm -rf site/ htmlcov/ .pytest_cache/ .ruff_cache/ dist/ build/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

# ─── Composite Targets ────────────────────────────────────────────────────────

check: lint format-check security gitleaks ## Run all code quality checks

ci: install lint format-check security test-cov docs ## Full CI pipeline

all: install check test docs ## Install + check + test + docs

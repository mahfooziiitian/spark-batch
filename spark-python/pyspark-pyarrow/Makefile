.PHONY: help setup install sync lint format format-check typecheck security test test-v test-cov run docs docs-build clean clean-all check ci

SHELL  := /usr/bin/env bash
UV     := uv
PYTHON := $(UV) run python
PYTEST := $(UV) run pytest
RUFF   := $(UV) run ruff
MYPY   := $(UV) run mypy
BANDIT := $(UV) run bandit

SRC    := src/
TESTS  := tests/
ALL    := $(SRC) $(TESTS)

# ──────────────────────────────────────────────
# Help
# ──────────────────────────────────────────────

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ──────────────────────────────────────────────
# Setup
# ──────────────────────────────────────────────

setup: install sync ## Full project setup (install uv + sync deps)

install: ## Install uv if not present
	@command -v $(UV) >/dev/null 2>&1 || { echo "Installing uv…"; curl -LsSf https://astral.sh/uv/install.sh | sh; }

sync: ## Sync project dependencies (including dev group)
	$(UV) sync --group dev

# ──────────────────────────────────────────────
# Code Quality
# ──────────────────────────────────────────────

lint: ## Run ruff linter
	$(RUFF) check $(ALL)

format: ## Auto-format code with ruff
	$(RUFF) format $(ALL)
	$(RUFF) check --fix $(ALL)

format-check: ## Check formatting without modifying files
	$(RUFF) format --check $(ALL)
	$(RUFF) check $(ALL)

typecheck: ## Run mypy type checking
	$(MYPY) $(SRC)

# ──────────────────────────────────────────────
# Security
# ──────────────────────────────────────────────

security: ## Run bandit security scan
	$(BANDIT) -r $(SRC) -c pyproject.toml

# ──────────────────────────────────────────────
# Testing
# ──────────────────────────────────────────────

test: ## Run the full test suite
	$(PYTEST) $(TESTS)

test-v: ## Run tests with verbose output
	$(PYTEST) $(TESTS) -v --tb=short

test-cov: ## Run tests with coverage report
	$(PYTEST) $(TESTS) --cov=$(SRC) --cov-report=term-missing

# ──────────────────────────────────────────────
# Run
# ──────────────────────────────────────────────

run: ## Run the main Arrow examples
	$(PYTHON) src/psa/pyspark_pyarrow.py

run-udf: ## Run the Pandas UDF examples
	$(PYTHON) src/psa/pandas_udf_spark.py

run-udtf: ## Run the UDTF examples
	$(PYTHON) src/psa/pyspark_udtf.py

# ──────────────────────────────────────────────
# Documentation
# ──────────────────────────────────────────────

docs: ## Serve docs locally with live reload
	$(UV) run mkdocs serve

docs-build: ## Build docs in strict mode
	NO_MKDOCS_2_WARNING=1 $(UV) run mkdocs build --strict

# ──────────────────────────────────────────────
# Cleanup
# ──────────────────────────────────────────────

clean: ## Remove caches and build artifacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	rm -rf site/ dist/ build/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

clean-all: clean ## Remove everything including .venv
	rm -rf .venv

# ──────────────────────────────────────────────
# Combined Targets
# ──────────────────────────────────────────────

check: lint typecheck security ## Run all code quality checks

ci: format-check typecheck security test docs-build ## Run full CI pipeline

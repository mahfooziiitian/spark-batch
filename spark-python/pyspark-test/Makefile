#!/usr/bin/env make -f
.DEFAULT_GOAL := help

# ─── Root Project ────────────────────────────────────────────────────────────

.PHONY: help install test lint format check security quality docs docs-serve clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install all dependencies (root + child projects)
	uv sync --group dev
	cd pyspark-chispa && uv sync --group dev
	cd pyspark-deepu && uv sync --group dev
	cd pyspark-pytest && uv sync --group dev

test: ## Run tests in all child projects
	cd pyspark-chispa && uv run task test
	cd pyspark-deepu && uv run task test
	cd pyspark-pytest && uv run task test

lint: ## Lint all child projects
	cd pyspark-chispa && uv run task lint
	cd pyspark-deepu && uv run task lint
	cd pyspark-pytest && uv run task lint

format: ## Format all child projects
	cd pyspark-chispa && uv run task format
	cd pyspark-deepu && uv run task format
	cd pyspark-pytest && uv run task format

security: ## Run security checks on all child projects
	cd pyspark-chispa && uv run task security
	cd pyspark-deepu && uv run task security
	cd pyspark-pytest && uv run task security

quality: ## Run all code quality checks (lint + format + typecheck + security)
	cd pyspark-chispa && make quality
	cd pyspark-deepu && make quality
	cd pyspark-pytest && make quality

check: ## Full CI pipeline (lint + test) for all projects
	cd pyspark-chispa && uv run task check
	cd pyspark-deepu && uv run task check
	cd pyspark-pytest && uv run task check

docs: ## Build documentation for all projects
	uv run mkdocs build --strict
	cd pyspark-chispa && uv run mkdocs build --strict
	cd pyspark-deepu && uv run mkdocs build --strict
	cd pyspark-pytest && uv run mkdocs build --strict

docs-serve: ## Serve root docs locally
	uv run mkdocs serve --livereload

clean: ## Remove caches and build artifacts from all projects
	rm -rf site .pytest_cache .mypy_cache .ruff_cache
	cd pyspark-chispa && uv run task clean
	cd pyspark-deepu && uv run task clean
	cd pyspark-pytest && uv run task clean

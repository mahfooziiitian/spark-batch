#!/usr/bin/env make -f
.DEFAULT_GOAL := help

.PHONY: help install test test-verbose test-parallel lint lint-fix format format-check typecheck security quality check examples docs docs-serve clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install all dependencies
	uv sync --group dev

test: ## Run tests (stop on first failure)
	uv run task test

test-verbose: ## Run tests with verbose output
	uv run task test_verbose

test-parallel: ## Run tests in parallel
	uv run task test_parallel

lint: ## Lint with ruff
	uv run task lint

lint-fix: ## Lint and auto-fix issues
	uv run task lint_fix

format: ## Format with ruff
	uv run task format

format-check: ## Check formatting without changes
	uv run task format_check

typecheck: ## Type check with mypy
	uv run task typecheck

security: ## Run security checks (bandit + safety)
	uv run task security
	uv run task security_deps

quality: ## Run all code quality checks (lint + format + typecheck + security)
	uv run task lint
	uv run task format_check
	uv run task typecheck
	uv run task security

check: ## Full CI pipeline (lint + format check + typecheck + security + test)
	uv run task check

docs: ## Build MkDocs documentation
	uv run task docs

docs-serve: ## Serve docs locally at localhost:8000
	uv run task docs_serve

examples: ## Run all example scripts
	@for f in examples/*.py; do echo "\n=== Running $$f ==="; PYTHONPATH=src uv run python $$f; done

clean: ## Remove caches and build artifacts
	uv run task clean

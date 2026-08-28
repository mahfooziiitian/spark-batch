.PHONY: help sync install test format format-check lint typecheck complexity security \
        gitleaks pip-audit precommit precommit-install docs docs-build docs-serve \
        build clean ci all

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

sync install: ## Install runtime + dev dependencies (uv sync)
	uv sync

test: ## Run the test suite
	uv run pytest tests/ -v --tb=short

format: ## Auto-format code (black + isort)
	uv run black src tests examples
	uv run isort src tests examples

format-check: ## Check formatting without modifying files
	uv run black --check src tests examples
	uv run isort --check-only src tests examples

lint: ## Style lint (flake8 + ruff)
	uv run flake8 src examples
	uv run ruff check src

typecheck: ## Static type check the library (mypy)
	uv run mypy src/rest_ds

complexity: ## Report cyclomatic complexity + maintainability index (radon)
	uv run radon cc src/rest_ds -a -nb
	uv run radon mi src/rest_ds -s

security: ## Static security scan (bandit)
	uv run bandit -r src -c pyproject.toml

gitleaks: ## Scan for hardcoded secrets/credentials (gitleaks; run `brew install gitleaks` or see https://github.com/gitleaks/gitleaks#installing if missing)
	gitleaks detect --source . --no-git -v

pip-audit: ## Scan dependencies for known CVEs
	uv run pip-audit

precommit-install: ## Install git pre-commit hooks
	uv run pre-commit install

precommit: ## Run all pre-commit hooks against the whole repo
	uv run pre-commit run --all-files

docs docs-build: ## Build the MkDocs site (strict mode)
	uv run mkdocs build --strict

docs-serve: ## Serve docs locally with live reload
	uv run mkdocs serve

build: ## Build the rest_ds wheel/sdist
	uv build

clean: ## Remove caches, build artifacts, and the docs site
	rm -rf .mypy_cache .pytest_cache site dist build
	find . -type d -name "__pycache__" -not -path "./.venv/*" -exec rm -rf {} +

ci: format-check lint typecheck security gitleaks test docs-build ## Run the full local CI pipeline
all: ci ## Alias for `ci`

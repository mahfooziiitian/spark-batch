.DEFAULT_GOAL := help

# ── Tooling ───────────────────────────────────────────────────────────────────
# Resolve the `uv` binary robustly. IDEs (e.g. the PyCharm GUI) launch `make`
# with a minimal PATH that does not source the shell profile, so `~/.local/bin`
# may be missing. Prefer `uv` on PATH; otherwise fall back to its default user
# install location. Override with `make UV=/path/to/uv <target>` if needed.
UV ?= $(shell command -v uv 2>/dev/null || echo $(HOME)/.local/bin/uv)

# ── Help ──────────────────────────────────────────────────────────────────────
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' \
		| sort

# ── Setup ─────────────────────────────────────────────────────────────────────
.PHONY: install
install: ## Sync the virtualenv from uv.lock
	$(UV) sync --all-groups

.PHONY: lock
lock: ## Re-resolve and update uv.lock
	$(UV) lock

# ── Tests ─────────────────────────────────────────────────────────────────────
.PHONY: test
test: ## Run pytest with verbose output
	$(UV) run pytest -vv tests

.PHONY: test-fast
test-fast: ## Stop at first failure, quiet output
	$(UV) run pytest -x -q tests

.PHONY: test-parallel
test-parallel: ## Run pytest in parallel across all CPU cores
	$(UV) run pytest -n auto -v --tb=short tests

.PHONY: test-cov
test-cov: ## Run pytest with terminal + HTML coverage report
	$(UV) run pytest --cov=src --cov-report=term-missing --cov-report=html:reports/htmlcov tests

# ── Quality ───────────────────────────────────────────────────────────────────
.PHONY: quality
quality: format lint type-check sql ## Run full quality pipeline

.PHONY: format
format: ## Format code and sort imports with ruff
	$(UV) run ruff check --select I --fix src tests
	$(UV) run ruff format src tests

.PHONY: format-check
format-check: ## Check formatting without modifying files
	$(UV) run ruff format --check src tests

.PHONY: lint
lint: ## Lint with ruff
	$(UV) run ruff check src tests

.PHONY: lint-fix
lint-fix: ## Lint with ruff, auto-fixing what it can
	$(UV) run ruff check --fix src tests

.PHONY: type-check
type-check: ## Static type check with mypy
	$(UV) run mypy src

.PHONY: sql
sql: sql-format sql-lint ## Fix then lint all SQL files

.PHONY: sql-format
sql-format: ## Auto-fix SQL with sqlfluff
	$(UV) run sqlfluff fix sql --force

.PHONY: sql-lint
sql-lint: ## Lint SQL with sqlfluff
	$(UV) run sqlfluff lint sql

# ── CI ────────────────────────────────────────────────────────────────────────
.PHONY: ci
ci: format-check lint type-check sql-lint test-cov docs-build ## Run the full CI pipeline (no auto-fixing)

.PHONY: pre-commit
pre-commit: quality docs-build test ## Run the recommended pre-commit gate

# ── Security ──────────────────────────────────────────────────────────────────
.PHONY: secure
secure: bandit safety ## Run all security checks

.PHONY: bandit
bandit: ## Static security scan with bandit
	$(UV) run bandit -c pyproject.toml -r src

.PHONY: safety
safety: ## Check dependencies for known CVEs
	$(UV) run safety check --policy-file .safety-policy.yml

# ── Reporting ─────────────────────────────────────────────────────────────────
.PHONY: report-html
report-html: ## Generate HTML test report
	$(UV) run pytest --html=reports/reports.html --self-contained-html

.PHONY: report-xml
report-xml: ## Generate JUnit XML test report
	$(UV) run pytest --junitxml=reports/report.xml

.PHONY: report-json
report-json: ## Generate JSON test report with coverage
	$(UV) run pytest --json-report --json-report-file=reports/report.json --cov-report=term-missing

.PHONY: report-cov-html
report-cov-html: ## Generate HTML coverage report
	$(UV) run pytest --cov=. --cov-report=html --cov-config=.coveragerc --cov-report=term-missing

.PHONY: report-cov-xml
report-cov-xml: ## Generate XML coverage report
	$(UV) run pytest --cov=. --cov-report=xml --cov-config=.coveragerc --cov-report=term-missing

# ── Documentation ─────────────────────────────────────────────────────────────
.PHONY: docs
docs: clean docs-build docs-serve ## Clean, build, then serve docs

.PHONY: docs-build
docs-build: ## Build MkDocs site
	NO_MKDOCS_2_WARNING=1 uv run mkdocs build --strict

.PHONY: docs-serve
docs-serve: ## Serve docs on 0.0.0.0:8080
	$(UV) run mkdocs serve -a 0.0.0.0:8000 --livereload

.PHONY: docs-lint
docs-lint: ## Lint and fix markdown files
	$(UV) run pymarkdownlnt fix -r docs README.md

# ── Build ─────────────────────────────────────────────────────────────────────
.PHONY: build
build: ## Build the package
	$(UV) build

# ── Clean ─────────────────────────────────────────────────────────────────────
.PHONY: clean
clean: ## Remove caches and build artefacts
	rm -rf site dist build
	rm -rf reports
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	rm -rf htmlcov .coverage .coverage.*
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@echo "✓ Clean complete"

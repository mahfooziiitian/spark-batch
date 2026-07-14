.DEFAULT_GOAL := help

# ── Help ──────────────────────────────────────────────────────────────────────
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' \
		| sort

# ── Tests ─────────────────────────────────────────────────────────────────────
.PHONY: test
test: ## Run pytest with verbose output
	uv run pytest -vv tests

# ── Quality ───────────────────────────────────────────────────────────────────
.PHONY: quality
quality: import format format-check lint type-check sql ## Run full quality pipeline

.PHONY: import
import: ## Sort imports with isort (black profile)
	uv run isort --profile black src tests

.PHONY: format
format: ## Format code with ruff
	uv run ruff format src tests

.PHONY: format-check
format-check: ## Lint with ruff (no fixes)
	uv run ruff check src tests

.PHONY: lint
lint: ## Lint with flake8
	uv run flake8 -v src tests

.PHONY: type-check
type-check: ## Static type check with mypy
	uv run mypy tests --disable-error-code index

.PHONY: sql
sql: sql-format sql-lint ## Fix then lint all SQL files

.PHONY: sql-format
sql-format: ## Auto-fix SQL with sqlfluff
	uv run sqlfluff fix src

.PHONY: sql-lint
sql-lint: ## Lint SQL with sqlfluff
	uv run sqlfluff lint src

# ── Security ──────────────────────────────────────────────────────────────────
.PHONY: secure
secure: bandit safety ## Run all security checks

.PHONY: bandit
bandit: ## Static security scan with bandit
	uv run bandit -c pyproject.toml -r src

.PHONY: safety
safety: ## Check dependencies for known CVEs
	uv run safety check --policy-file .safety-policy.yml

# ── Reporting ─────────────────────────────────────────────────────────────────
.PHONY: report-html
report-html: ## Generate HTML test report
	uv run pytest --html=reports/reports.html --self-contained-html

.PHONY: report-xml
report-xml: ## Generate JUnit XML test report
	uv run pytest --junitxml=reports/report.xml

.PHONY: report-json
report-json: ## Generate JSON test report with coverage
	uv run pytest --json-report --json-report-file=reports/report.json --cov-report=term-missing

.PHONY: report-cov-html
report-cov-html: ## Generate HTML coverage report
	uv run pytest --cov=. --cov-report=html --cov-config=.coveragerc --cov-report=term-missing

.PHONY: report-cov-xml
report-cov-xml: ## Generate XML coverage report
	uv run pytest --cov=. --cov-report=xml --cov-config=.coveragerc --cov-report=term-missing

# ── Documentation ─────────────────────────────────────────────────────────────
.PHONY: docs
docs: clean docs-build docs-serve ## Clean, build, then serve docs

.PHONY: docs-build
docs-build: ## Build MkDocs site
	NO_MKDOCS_2_WARNING=1 uv run mkdocs build --strict

.PHONY: docs-serve
docs-serve: ## Serve docs on 0.0.0.0:8080
	uv run mkdocs serve -a 0.0.0.0:8000 --livereload

.PHONY: lint-md
lint-md: ## Lint markdown files
	uv run pymarkdownlnt fix -r docs README.md

# ── Build ─────────────────────────────────────────────────────────────────────
.PHONY: build
build: ## Build the package
	uv build

# ── Clean ─────────────────────────────────────────────────────────────────────
.PHONY: clean
clean: ## Remove caches and build artefacts
	rm -rf __pycache__ .pytest_cache site

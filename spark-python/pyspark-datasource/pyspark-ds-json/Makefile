.PHONY: help install sync dev test test-fast test-cov lint format type-check security audit license-check compile check check-all docs docs-lint docs-serve docs-deploy build clean run-example generate-data run-examples submit-local docker-run

# Default target
help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Setup ────────────────────────────────────────────────────────────────────

install: ## Install all dependencies (uv sync)
	uv sync

sync: install ## Alias for install

dev: ## Install with dev dependencies and all extras
	uv sync --all-extras

# ─── Testing ──────────────────────────────────────────────────────────────────

test: ## Run all tests
	uv run pytest tests/ -v

test-fast: ## Run tests without Spark startup overhead (markers)
	uv run pytest tests/ -v -m "not slow"

test-cov: ## Run tests with coverage report
	uv run pytest tests/ -v --cov=pys_json --cov-report=term-missing --cov-report=html:htmlcov

# ─── Code Quality ─────────────────────────────────────────────────────────────

lint: ## Run ruff linter
	uv run ruff check src/ examples/ tests/

lint-fix: ## Run ruff linter with auto-fix
	uv run ruff check --fix src/ examples/ tests/

format: ## Format code with ruff
	uv run ruff format src/ examples/ tests/

format-check: ## Check formatting without changing files
	uv run ruff format --check src/ examples/ tests/

type-check: ## Run mypy type checking
	uv run mypy src/pys_json/ --ignore-missing-imports

compile: ## Compile all Python files (syntax check)
	uv run python -m compileall src/ examples/ tests/ -q

# ─── Security & Auditing ─────────────────────────────────────────────────────

security: ## Run bandit security scanner on source code
	uv run bandit -r src/ -c pyproject.toml -q

security-full: ## Run bandit on source + examples with verbose output
	uv run bandit -r src/ examples/ -c pyproject.toml

audit: ## Audit dependencies for known vulnerabilities
	uv run pip-audit

license-check: ## Check dependency licenses for compatibility
	uv run pip-licenses --order=license --format=table

secrets-scan: ## Scan for accidentally committed secrets
	@echo "Scanning for potential secrets..."
	@grep -rn --include="*.py" --include="*.yml" --include="*.yaml" --include="*.toml" \
		-E "(password|secret|api_key|token|credential)\s*=" src/ examples/ scripts/ 2>/dev/null \
		&& echo "⚠  Potential secrets found — review above" && exit 1 \
		|| echo "✓ No secrets detected"

# ─── Combined Checks ─────────────────────────────────────────────────────────

check: lint compile ## Quick checks (lint + compile)

check-all: format-check lint type-check compile security secrets-scan ## Full CI pipeline (all checks)

ci: check-all test docs docs-lint ## Complete CI run (checks + tests + docs)

# ─── Documentation ────────────────────────────────────────────────────────────

docs: ## Build MkDocs documentation
	uv run mkdocs build --strict

docs-lint: ## Lint Markdown documentation
	uv run pymarkdown --config .pymarkdown.json scan docs/

docs-serve: ## Serve docs locally (http://localhost:8000)
	uv run mkdocs serve

docs-deploy: ## Deploy docs to GitHub Pages
	uv run mkdocs gh-deploy --force

# ─── Build & Package ──────────────────────────────────────────────────────────

build: clean ## Build wheel and sdist
	uv build

# ─── Data & Examples ──────────────────────────────────────────────────────────

generate-data: ## Generate test JSON fixture files
	uv run python generate_test_data.py

run-example: ## Run a specific example (usage: make run-example EX=examples/06_schema/01_struct_type_schema.py)
	uv run python $(EX)

run-examples: ## Run all examples (or category: make run-examples CAT=06_schema)
	./scripts/run-all-examples.sh $(CAT)

submit-local: ## spark-submit locally (usage: make submit-local EX=examples/06_schema/01_struct_type_schema.py)
	./scripts/spark-submit-local.sh $(EX)

docker-run: ## Run example in Docker (usage: make docker-run EX=examples/06_schema/01_struct_type_schema.py)
	./scripts/docker-run.sh $(EX)

# ─── Cleanup ──────────────────────────────────────────────────────────────────

clean: ## Remove build artifacts, caches, and generated docs
	rm -rf dist/ build/ site/ .ruff_cache/ htmlcov/ .mypy_cache/ .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

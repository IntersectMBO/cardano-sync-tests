.DEFAULT_GOAL := help

PIP_INSTALL_ARGS ?=
VENV := .venv
PY := $(VENV)/bin/python3
PIP := $(PY) -m pip
PYTEST_ARGS ?=

.PHONY: .check-venv-exists
.check-venv-exists:
	@if [ ! -d "$(VENV)" ]; then \
		echo "Error: Virtual environment not found. Please run 'make install' first." >&2; \
		exit 1; \
	fi

.PHONY: .check-venv-not-activated
.check-venv-not-activated:
	@venv_abs="$$(readlink -f -- "$(CURDIR)/$(VENV)" 2>/dev/null || echo "$(CURDIR)/$(VENV)")"; \
		actual="$$(readlink -f -- "$${VIRTUAL_ENV:-}" 2>/dev/null || echo "$${VIRTUAL_ENV:-}")"; \
		if [ -n "$${VIRTUAL_ENV:-}" ] && [ "$$actual" = "$$venv_abs" ]; then \
			echo "Error: Project virtual environment is currently activated. Please deactivate it first." >&2; \
			exit 1; \
		fi

## ---------------------------------------------------------------------------
## Setup
## ---------------------------------------------------------------------------

.PHONY: install
install: ## Install cardano-sync-tests and its dev dependencies into a virtual environment
	@venv_abs="$$(readlink -f -- "$(CURDIR)/$(VENV)" 2>/dev/null || echo "$(CURDIR)/$(VENV)")"; \
		actual="$$(readlink -f -- "$${VIRTUAL_ENV:-}" 2>/dev/null || echo "$${VIRTUAL_ENV:-}")"; \
		if [ -n "$${VIRTUAL_ENV:-}" ] && [ "$$actual" != "$$venv_abs" ]; then \
			echo "Error: Another virtual environment is currently activated. Please deactivate it before running 'make install'." >&2; \
			exit 1; \
		fi
	@if [ ! -x "$(PY)" ]; then \
		python3 -m venv $(VENV); \
	fi
	@if ! $(PY) -m pip --version >/dev/null 2>&1; then \
		echo "No pip in $(VENV), bootstrapping it with ensurepip"; \
		$(PY) -m ensurepip --upgrade; \
	fi
	$(PIP) install --require-virtualenv --upgrade pip
	$(PIP) install --require-virtualenv --upgrade -e . --group dev $(PIP_INSTALL_ARGS)
	@echo ""
	@echo "Virtual environment ready. Activate with: source $(VENV)/bin/activate"

## ---------------------------------------------------------------------------
## Linting
## ---------------------------------------------------------------------------

.PHONY: init-lint
init-lint: .check-venv-exists ## Initialize linters
	$(VENV)/bin/pre-commit clean
	$(VENV)/bin/pre-commit gc
	find . -path '*/.mypy_cache/*' -delete
	$(VENV)/bin/pre-commit uninstall
	$(VENV)/bin/pre-commit install --install-hooks

.PHONY: lint
lint: .check-venv-exists ## Run linters
	$(VENV)/bin/pre-commit run -a --show-diff-on-failure --color=always

## ---------------------------------------------------------------------------
## Testing
## ---------------------------------------------------------------------------

.PHONY: test
test: .check-venv-exists ## Run framework unit tests (no synced node needed)
	$(PY) -m pytest framework_tests $(PYTEST_ARGS)

## ---------------------------------------------------------------------------
## Maintenance
## ---------------------------------------------------------------------------

.PHONY: clean
clean: ## Clean build artifacts and caches
	find . -type d -name __pycache__ -not -path './$(VENV)/*' -exec rm -rf {} +
	find . -type d -name .pytest_cache -not -path './$(VENV)/*' -exec rm -rf {} +
	find . -type d -name .mypy_cache -not -path './$(VENV)/*' -exec rm -rf {} +
	find . -type d -name '*.egg-info' -not -path './$(VENV)/*' -exec rm -rf {} +
	find . -name '*.pyc' -not -path './$(VENV)/*' -delete

.PHONY: clean-all
clean-all: .check-venv-not-activated clean ## Clean all build artifacts, caches, and virtual environment
	@echo "Removing virtual environment: $(VENV)"
	rm -rf -- "$(VENV)"

## ---------------------------------------------------------------------------
## Help
## ---------------------------------------------------------------------------

.PHONY: help
help: ## Show this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} \
		/^## [A-Z][a-zA-Z]*$$/ { section = substr($$0, 4); next } \
		/^[a-zA-Z_-]+:.*##/ { \
			if (section != last_section) { \
				printf "\n\033[1m%s\033[0m\n", section; \
				last_section = section; \
			} \
			printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2; \
		}' \
		$(MAKEFILE_LIST)

.PHONY: help setup test test-verbose test-coverage lint lint-fix format format-check clean ui-setup ui-build ui-dev

# Airflow 3.x requires Python 3.9+
PYTHON := $(shell which python3.12 || which python3.11 || which python3.10 || which python3.9 || which python3)

# Use venv binaries if they exist, otherwise use PATH
PYTEST := $(shell test -f ./venv/bin/pytest && echo ./venv/bin/pytest || echo pytest)
RUFF := $(shell test -f ./venv/bin/ruff && echo ./venv/bin/ruff || echo ruff)

UI_DIR := plugins/big_red_button/ui

help:
	@echo "Available commands:"
	@echo "  make setup          - Create virtual environment and install dependencies"
	@echo "  make test           - Run tests"
	@echo "  make test-verbose   - Run tests with verbose output"
	@echo "  make test-coverage  - Run tests with coverage report"
	@echo "  make lint           - Run ruff linter"
	@echo "  make lint-fix       - Run ruff linter and auto-fix issues"
	@echo "  make format         - Format code with ruff"
	@echo "  make format-check   - Check code formatting without making changes"
	@echo "  make ui-setup       - Install UI dependencies"
	@echo "  make ui-build       - Build UI bundle for production"
	@echo "  make ui-dev         - Start UI dev server with hot reload"
	@echo "  make clean          - Remove virtual environment and cache files"

setup:
	@echo "Using Python: $(PYTHON)"
	@$(PYTHON) --version
	$(PYTHON) -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -r requirements-dev.txt
	@echo ""
	@echo "Setup complete! Activate the virtual environment with:"
	@echo "  source venv/bin/activate"

test:
	$(PYTEST) tests/

test-verbose:
	$(PYTEST) tests/ -v

test-coverage:
	$(PYTEST) tests/ --cov=plugins/big_red_button --cov-report=term-missing

lint:
	$(RUFF) check .

lint-fix:
	$(RUFF) check --fix .

format:
	$(RUFF) format .

format-check:
	$(RUFF) format --check .

ui-setup:
	cd $(UI_DIR) && npm install

ui-build:
	cd $(UI_DIR) && npm run build

ui-dev:
	cd $(UI_DIR) && npm run dev

clean:
	rm -rf venv
	rm -rf $(UI_DIR)/node_modules
	rm -rf plugins/big_red_button/static
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

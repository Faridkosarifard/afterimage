PYTHON ?= python3
VENV ?= .venv
BIN := $(VENV)/bin
COMPOSE := docker compose -f demo/docker-compose.yml

.PHONY: help install lint format typecheck test cov check demo-up demo demo-down demo-local clean

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

install:  ## Create a virtualenv and install the package with dev dependencies
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -e ".[dev]"

lint:  ## Run ruff lint and format checks
	$(BIN)/ruff check src tests demo
	$(BIN)/ruff format --check src tests demo

format:  ## Auto-format and fix lint issues
	$(BIN)/ruff format src tests demo
	$(BIN)/ruff check --fix src tests demo

typecheck:  ## Run mypy in strict mode
	$(BIN)/mypy

test:  ## Run the test suite
	$(BIN)/pytest

cov:  ## Run tests with coverage
	$(BIN)/pytest --cov --cov-report=term-missing --cov-report=xml

check: lint typecheck test  ## Everything CI runs

demo-up:  ## Start MinIO (versioned bucket) and an Iceberg REST catalog
	$(COMPOSE) up -d --wait rest

demo:  ## Run the walkthrough against the docker-compose services
	$(BIN)/python demo/demo.py

demo-down:  ## Stop the playground and delete its data
	$(COMPOSE) down -v

demo-local:  ## Run the walkthrough without Docker (moto S3 + sqlite catalog)
	$(BIN)/python demo/run_with_moto.py

clean:  ## Remove caches and build artefacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage coverage.xml htmlcov dist build demo/out

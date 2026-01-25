.PHONY: help install install-proxy run run-proxy lint format mypy check-all clean

help: ## Show available targets
	@echo "\033[1m📊 Klines Streamer & Proxy\033[0m"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies using poetry
	@echo "📦 Installing dependencies..."
	@poetry install

install-proxy: ## Install proxy dependencies
	@echo "📦 Installing proxy dependencies..."
	@poetry install --with proxy

run: ## Run the Bybit klines streamer
	@echo "🚀 Running Bybit klines streamer..."
	@poetry run python -m streamer

run-proxy: ## Run the proxy server
	@echo "🚀 Running proxy server..."
	@poetry run python -m proxy

lint-streamer: ## Lint the streamer code with Ruff
	@echo "🔍 Linting code (streamer)..."
	@poetry run ruff check streamer

lint-proxy: ## Lint the proxy code with Ruff
	@echo "🔍 Linting code (proxy)..."
	@poetry run ruff check proxy

format-streamer: ## Format the streamer code with Ruff
	@echo "🖋️ Formatting code (streamer)..."
	@poetry run ruff format streamer

format-proxy: ## Format the proxy code with Ruff
	@echo "🖋️ Formatting code (proxy)..."
	@poetry run ruff format proxy

mypy-streamer: ## Check types in streamer with MyPy
	@echo "🔍 Checking types with MyPy (streamer)..."
	@poetry run mypy streamer

mypy-proxy: ## Check types in proxy with MyPy
	@echo "🔍 Checking types with MyPy (proxy)..."
	@poetry run mypy proxy

lint: lint-streamer lint-proxy ## Lint the code (streamer & proxy)
format: format-streamer format-proxy ## Format the code (streamer & proxy)
mypy: mypy-streamer mypy-proxy ## Check types with MyPy (streamer & proxy)

check-all: format-streamer format-proxy lint-streamer lint-proxy mypy-streamer mypy-proxy ## Run all linters (streamer then proxy)

clean: ## Clean temporary files
	@echo "🧹 Cleaning temporary files..."
	@find . -type f -name '*.pyc' -delete
	@find . -type d -name '__pycache__' -exec rm -rf {} +

%:
	@:

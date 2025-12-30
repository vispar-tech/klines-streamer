.PHONY: help install run lint format mypy check-all clean

help: ## Show available targets
	@echo "\033[1m📊 Bybit Klines Streamer\033[0m"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies using poetry
	@echo "📦 Installing dependencies..."
	@poetry install

run: ## Run the Bybit klines streamer
	@echo "🚀 Running Bybit klines streamer..."
	@poetry run python -m streamer

lint: ## Lint the code with Ruff
	@echo "🔍 Linting code..."
	@poetry run ruff check streamer

format: ## Format the code with Ruff
	@echo "🖋️ Formatting code..."
	@poetry run ruff format streamer

mypy: ## Check types with MyPy
	@echo "🔍 Checking types with MyPy..."
	@poetry run mypy streamer

check-all: format lint mypy ## Run all linters

clean: ## Clean temporary files
	@echo "🧹 Cleaning temporary files..."
	@find . -type f -name '*.pyc' -delete
	@find . -type d -name '__pycache__' -exec rm -rf {} +

%:
	@:

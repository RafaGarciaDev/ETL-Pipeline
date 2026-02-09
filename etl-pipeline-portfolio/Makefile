.PHONY: help setup install up down logs test clean run

# Colors
BLUE = \033[0;34m
GREEN = \033[0;32m
RED = \033[0;31m
NC = \033[0m

help: ## Show this help message
	@echo "$(BLUE)ETL Pipeline - Available Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

setup: ## Initial setup
	@echo "$(BLUE)Setting up ETL pipeline...$(NC)"
	@cp -n .env.example .env 2>/dev/null || true
	@mkdir -p data/raw data/processed data/logs
	@echo "$(GREEN)✓ Setup complete$(NC)"

install: ## Install Python dependencies
	@echo "$(BLUE)Installing dependencies...$(NC)"
	@pip install -r requirements.txt
	@echo "$(GREEN)✓ Dependencies installed$(NC)"

up: ## Start Docker containers
	@echo "$(BLUE)Starting containers...$(NC)"
	@docker-compose up -d
	@echo "$(GREEN)✓ Containers started$(NC)"
	@echo "Airflow UI: http://localhost:8080"
	@echo "PostgreSQL: localhost:5432"

down: ## Stop Docker containers
	@echo "$(BLUE)Stopping containers...$(NC)"
	@docker-compose down
	@echo "$(GREEN)✓ Containers stopped$(NC)"

logs: ## Show container logs
	@docker-compose logs -f

restart: down up ## Restart containers

run: ## Run ETL pipeline locally
	@echo "$(BLUE)Running ETL pipeline...$(NC)"
	@python src/main.py
	@echo "$(GREEN)✓ Pipeline completed$(NC)"

run-extract: ## Run only extraction
	@echo "$(BLUE)Running extraction...$(NC)"
	@python src/main.py --source csv
	@echo "$(GREEN)✓ Extraction completed$(NC)"

test: ## Run tests
	@echo "$(BLUE)Running tests...$(NC)"
	@pytest tests/ -v --cov=src --cov-report=html
	@echo "$(GREEN)✓ Tests completed$(NC)"

test-unit: ## Run unit tests only
	@pytest tests/test_extract.py tests/test_transform.py tests/test_load.py -v

lint: ## Run code linting
	@echo "$(BLUE)Running linting...$(NC)"
	@flake8 src/ --max-line-length=100
	@black src/ --check
	@echo "$(GREEN)✓ Linting complete$(NC)"

format: ## Format code with black
	@echo "$(BLUE)Formatting code...$(NC)"
	@black src/
	@echo "$(GREEN)✓ Code formatted$(NC)"

clean: ## Clean generated files
	@echo "$(BLUE)Cleaning...$(NC)"
	@find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete
	@rm -rf .pytest_cache htmlcov .coverage
	@rm -rf data/logs/*.log
	@echo "$(GREEN)✓ Cleaned$(NC)"

db-init: ## Initialize database
	@echo "$(BLUE)Initializing database...$(NC)"
	@docker-compose exec postgres psql -U etl_user -d etl_warehouse -f /docker-entrypoint-initdb.d/init.sql
	@echo "$(GREEN)✓ Database initialized$(NC)"

db-shell: ## Open database shell
	@docker-compose exec postgres psql -U etl_user -d etl_warehouse

airflow-init: ## Initialize Airflow
	@echo "$(BLUE)Initializing Airflow...$(NC)"
	@docker-compose run --rm airflow-init
	@echo "$(GREEN)✓ Airflow initialized$(NC)"

validate-config: ## Validate configuration
	@echo "$(BLUE)Validating configuration...$(NC)"
	@python -c "from src.utils.config import Config; c = Config(); print('Configuration valid')"
	@echo "$(GREEN)✓ Configuration valid$(NC)"

generate-sample-data: ## Generate sample data
	@echo "$(BLUE)Generating sample data...$(NC)"
	@python scripts/generate_sample_data.py
	@echo "$(GREEN)✓ Sample data generated$(NC)"

status: ## Show status of services
	@docker-compose ps

full-setup: setup install up airflow-init ## Complete setup from scratch
	@echo "$(GREEN)✓ Full setup complete!$(NC)"
	@echo "Access Airflow at http://localhost:8080 (airflow/airflow)"

.PHONY: help infra streaming batch all up down logs clean clean-network status

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

.env:
	cp .env.example .env
	@echo "Created .env from .env.example"

infra: .env ## Start infrastructure services (postgres, kafka, minio, ml-api, adminer, kafka-ui, airflow)
	docker-compose up -d postgres flyway adminer minio minio-init ml-api kafka kafka-init kafka-ui airflow

streaming: .env ## Start streaming pipeline (producer-1, producer-2, consumer)
	docker-compose --profile streaming up -d

batch: .env ## Start batch pipeline
	docker-compose --profile batch up -d

all: .env ## Start all services (infra + streaming + batch)
	docker-compose --profile all up -d

up: infra ## Alias for 'infra' - start infrastructure only
	@echo "Infrastructure started!"

down: ## Stop all services (keeps volumes)
	docker-compose --profile all down --remove-orphans

down-clean: ## Stop all services and remove volumes (complete cleanup)
	docker-compose --profile all down -v --remove-orphans
	docker network prune -f

status: ## Show status of all containers
	@echo "=== Container Status ==="
	@docker-compose ps
	@echo ""
	@echo "=== Network Status ==="
	@docker network ls | grep -E "NETWORK ID|transaction-enrichment" || echo "No project networks found"

logs-all: ## Follow all pipeline services logs (streaming + batch)
	docker-compose logs -f streaming-producer-1 streaming-producer-2 streaming-consumer batch

logs-streaming: ## Follow streaming services logs
	docker-compose logs -f streaming-producer-1 streaming-producer-2 streaming-consumer

logs-batch: ## Follow batch service logs
	docker-compose logs -f batch

logs-consumer: ## Follow consumer logs
	docker-compose logs -f streaming-consumer

logs-producers: ## Follow producer logs
	docker-compose logs -f streaming-producer-1 streaming-producer-2

logs-ml-api: ## Follow ML API logs
	docker-compose logs -f ml-api

logs-airflow: ## Follow Airflow logs
	docker-compose logs -f airflow

logs: ## Follow all logs
	docker-compose logs -f

clean: ## Stop and remove all containers, networks, volumes (safe cleanup)
	docker-compose --profile all down -v --remove-orphans
	docker network prune -f
	@echo "✓ Cleaned up project containers, networks, and volumes"

clean-all: ## Deep clean - removes all unused Docker resources (use with caution)
	docker-compose --profile all down -v --remove-orphans
	docker system prune -af --volumes
	@echo "✓ Deep cleaned all unused Docker resources"

clean-network: ## Fix network issues by removing orphaned containers and networks
	@echo "Stopping all containers..."
	-docker-compose --profile all down --remove-orphans 2>/dev/null || true
	@echo "Removing orphaned containers..."
	-docker ps -aq --filter "label=com.docker.compose.project=transaction-enrichment-pipeline" | xargs -r docker rm -f 2>/dev/null || true
	@echo "Cleaning networks..."
	-docker network prune -f
	@echo "✓ Network cleanup complete. Run 'make all' to restart services."

rebuild-streaming: ## Rebuild and restart streaming services
	docker-compose --profile streaming up -d --build

rebuild-batch: ## Rebuild and restart batch service
	docker-compose --profile batch up -d --build

rebuild-all: ## Rebuild and restart all pipeline services (streaming + batch)
	docker-compose --profile all up -d --build

rebuild-clean: ## Rebuild all services from scratch (no cache)
	docker-compose --profile all build --no-cache
	docker-compose --profile all up -d

restart-consumer: ## Restart consumer
	docker-compose restart streaming-consumer

restart-producers: ## Restart producers
	docker-compose restart streaming-producer-1 streaming-producer-2

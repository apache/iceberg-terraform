.PHONY: test-integration test-integration-setup test-integration-exec test-integration-cleanup

test-integration: test-integration-setup test-integration-exec test-integration-cleanup ## Run integration tests

test-integration-setup: ## Start Docker services for integration tests
	docker compose -f dev/docker-compose.yml kill
	docker compose -f dev/docker-compose.yml rm -f
	docker compose -f dev/docker-compose.yml up -d --wait

test-integration-exec: ## Run integration tests
	TF_ACC=1 ICEBERG_CATALOG_URI=http://localhost:8181 go test ./... -v

test-integration-cleanup: ## Clean up integration test environment
	@if [ "${KEEP_COMPOSE}" != "1" ]; then \
		echo "Cleaning up Docker containers..."; \
		docker compose -f dev/docker-compose.yml down; \
	fi

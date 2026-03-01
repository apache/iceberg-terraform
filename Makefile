# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: test-integration test-integration-setup test-integration-exec test-integration-cleanup

test-integration: test-integration-setup test-integration-exec test-integration-cleanup ## Run integration tests

test-integration-setup: ## Start Docker services for integration tests
	docker compose -f dev/docker-compose.yml kill
	docker compose -f dev/docker-compose.yml rm -f
	docker compose -f dev/docker-compose.yml up -d --wait

test-integration-exec: ## Run integration tests (Iceberg REST at 8181, Polaris at 8191)
	POLARIS_TOKEN=$$(curl -sf http://localhost:8191/api/catalog/v1/oauth/tokens --user root:s3cr3t -d grant_type=client_credentials -d 'scope=PRINCIPAL_ROLE:ALL' | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('access_token',''))") && \
	TF_ACC=1 ICEBERG_CATALOG_URI=http://localhost:8181 POLARIS_CATALOG_URI=http://localhost:8191 POLARIS_TOKEN="$$POLARIS_TOKEN" go test ./... -v

test-integration-cleanup: ## Clean up integration test environment
	@if [ "${KEEP_COMPOSE}" != "1" ]; then \
		echo "Cleaning up Docker containers..."; \
		docker compose -f dev/docker-compose.yml down; \
	fi

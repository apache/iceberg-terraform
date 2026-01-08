// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package provider

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccIcebergTable(t *testing.T) {
	mockServer := &mockIcebergRESTServerTable{}
	server := httptest.NewServer(mockServer)
	defer server.Close()

	providerCfg := fmt.Sprintf(providerConfig, server.URL)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccIcebergTableResourceConfig(providerCfg, "test_table"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("iceberg_table.test", "namespace.0", "db1"),
					resource.TestCheckResourceAttr("iceberg_table.test", "name", "test_table"),
				),
			},
		},
	})
}

type mockIcebergRESTServerTable struct {
	mu     sync.Mutex
	tables map[string]string // fqn -> "present"
}

func (s *mockIcebergRESTServerTable) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.tables == nil {
		s.tables = make(map[string]string)
	}

	path := r.URL.Path
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(parts) >= 4 && parts[0] == "v1" && parts[1] == "namespaces" && parts[3] == "tables" {
		namespace := parts[2]

		switch r.Method {
		case http.MethodPost: // Create table
			var reqBody struct {
				Name string `json:"name"`
			}
			json.NewDecoder(r.Body).Decode(&reqBody)

			fqn := namespace + "." + reqBody.Name
			s.tables[fqn] = "present"

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"metadata-location": "...", "metadata": {"properties":{}}}`)

		case http.MethodGet: // Load table
			tableName := parts[4]
			fqn := namespace + "." + tableName
			if _, ok := s.tables[fqn]; !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"metadata-location": "...", "metadata": {"properties":{}}}`)

		case http.MethodDelete: // Drop table
			tableName := parts[4]
			fqn := namespace + "." + tableName
			delete(s.tables, fqn)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	} else if len(parts) >= 3 && parts[0] == "v1" && parts[1] == "namespaces" { // for namespace existence check
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"properties":{}}`)
	} else {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Path not found: %s", path)
	}
}

func testAccIcebergTableResourceConfig(providerCfg string, tableName string) string {
	return providerCfg + fmt.Sprintf(`
resource "iceberg_table" "test" {
  namespace = ["db1"]
  name      = "%s"
}
`, tableName)
}

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
	"os"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func testAccPolarisProviderConfig(catalogURI, managementURI string) string {
	return testAccPolarisProviderConfigWithToken(catalogURI, managementURI, "")
}

func testAccPolarisProviderConfigWithToken(catalogURI, managementURI, token string) string {
	tokenAttr := ""
	if token != "" {
		tokenAttr = fmt.Sprintf("\n  token = %q", token)
	}
	return fmt.Sprintf(`
provider "iceberg" {
  catalog_uri            = "%s"
  polaris_management_uri = "%s"%s
}
`, catalogURI, managementURI, tokenAttr)
}

func newPolarisPrincipalTestServer(t *testing.T) *httptest.Server {
	t.Helper()

	principals := make(map[string]polarisPrincipal)

	mux := http.NewServeMux()

	mux.HandleFunc("/api/management/v1/principals", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req polarisCreatePrincipalRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		name := req.Principal.Name
		if name == "" {
			http.Error(w, "missing name", http.StatusBadRequest)
			return
		}

		p := polarisPrincipal{
			Name:          name,
			Properties:    req.Principal.Properties,
			EntityVersion: 1,
		}
		principals[name] = p

		resp := polarisPrincipalWithCredentials{
			Principal: p,
		}
		resp.Credentials.ClientID = "id-" + name
		resp.Credentials.ClientSecret = "secret-" + name

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/api/management/v1/principals/", func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/api/management/v1/principals/")
		if name == "" {
			http.NotFound(w, r)
			return
		}

		switch r.Method {
		case http.MethodGet:
			p, ok := principals[name]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(p)
		case http.MethodDelete:
			if _, ok := principals[name]; !ok {
				http.NotFound(w, r)
				return
			}
			delete(principals, name)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	return httptest.NewServer(mux)
}

func TestAccPolarisPrincipal(t *testing.T) {
	t.Parallel()

	srv := newPolarisPrincipalTestServer(t)
	defer srv.Close()

	providerCfg := testAccPolarisProviderConfig(
		"http://example.invalid",
		srv.URL+"/api/management/v1",
	)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerCfg + `
resource "iceberg_polaris_principal" "test" {
  name = "alice"

  properties = {
    team = "data"
  }
}
`,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("iceberg_polaris_principal.test", "name", "alice"),
					resource.TestCheckResourceAttr("iceberg_polaris_principal.test", "properties.team", "data"),
					resource.TestCheckResourceAttr("iceberg_polaris_principal.test", "client_id", "id-alice"),
				),
			},
		},
	})
}

// TestAccPolarisPrincipal_Full runs against a real Polaris deployment
func TestAccPolarisPrincipal_Full(t *testing.T) {
	catalogURI := os.Getenv("POLARIS_CATALOG_URI")
	if catalogURI == "" {
		t.Skip("POLARIS_CATALOG_URI not set, skipping real-cluster principal test")
	}
	managementURI := strings.TrimRight(catalogURI, "/") + "/api/management/v1"
	token := os.Getenv("POLARIS_TOKEN")

	providerCfg := testAccPolarisProviderConfigWithToken(catalogURI, managementURI, token)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerCfg + `
resource "iceberg_polaris_principal" "test" {
  name = "principal-real"

  properties = {
    team = "data"
  }
}
`,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("iceberg_polaris_principal.test", "name", "principal-real"),
					resource.TestCheckResourceAttr("iceberg_polaris_principal.test", "properties.team", "data"),
					resource.TestCheckResourceAttrSet("iceberg_polaris_principal.test", "client_id"),
					resource.TestCheckResourceAttrSet("iceberg_polaris_principal.test", "client_secret"),
				),
			},
		},
	})
}

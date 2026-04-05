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
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

// TestAccPolarisPrincipalRole_Full runs against a real Polaris deployment.
func TestAccPolarisPrincipalRole_Full(t *testing.T) {
	catalogURI := os.Getenv("POLARIS_CATALOG_URI")
	if catalogURI == "" {
		t.Skip("POLARIS_CATALOG_URI not set, skipping real-cluster principal role test")
	}

	managementURI := strings.TrimRight(catalogURI, "/") + "/api/management/v1"
	token := os.Getenv("POLARIS_TOKEN")

	providerCfg := testAccPolarisProviderConfigWithToken(catalogURI, managementURI, token)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	roleName := "tf-pr-role-" + suffix

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: providerCfg + fmt.Sprintf(`
resource "iceberg_polaris_principal_role" "test" {
  name = %q
  properties = {
    team        = "data"
    environment = "integration"
    owner       = "terraform"
  }
}
`, roleName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("iceberg_polaris_principal_role.test", "name", roleName),
					resource.TestCheckResourceAttr("iceberg_polaris_principal_role.test", "properties.team", "data"),
					resource.TestCheckResourceAttr("iceberg_polaris_principal_role.test", "properties.environment", "integration"),
					resource.TestCheckResourceAttr("iceberg_polaris_principal_role.test", "properties.owner", "terraform"),
					resource.TestCheckResourceAttr("iceberg_polaris_principal_role.test", "properties.%", "3"),
				),
			},
		},
	})
}

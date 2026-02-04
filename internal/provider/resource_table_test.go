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
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccIcebergTable(t *testing.T) {
	catalogURI := os.Getenv("ICEBERG_CATALOG_URI")
	if catalogURI == "" {
		catalogURI = "http://localhost:8181"
	}

	providerCfg := fmt.Sprintf(providerConfig, catalogURI)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			{
				Config: testAccIcebergTableResourceConfig(providerCfg, "test_table"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("iceberg_table.test", "namespace.0", "db1"),
					resource.TestCheckResourceAttr("iceberg_table.test", "name", "test_table"),
					resource.TestCheckResourceAttr("iceberg_table.test", "schema.fields.0.name", "id"),
					resource.TestCheckResourceAttr("iceberg_table.test", "schema.fields.0.type", "long"),
				),
			},
		},
	})
}

func testAccIcebergTableResourceConfig(providerCfg string, tableName string) string {
	return providerCfg + fmt.Sprintf(`
resource "iceberg_namespace" "db1" {
  name = ["db1"]
}

resource "iceberg_table" "test" {
  namespace = iceberg_namespace.db1.name
  name      = "%s"
  schema = {
    fields = [
      {
        id       = 1
        name     = "id"
        type     = "long"
        required = true
      },
      {
        id       = 2
        name     = "data"
        type     = "string"
        required = false
      }
    ]
  }
}
`, tableName)
}

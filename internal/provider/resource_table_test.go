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

func TestAccIcebergTableFull(t *testing.T) {
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
				Config: testAccIcebergTableFullConfig(providerCfg, "full_test_table"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("iceberg_table.full", "name", "full_test_table"),
					resource.TestCheckResourceAttr("iceberg_table.full", "namespace.0", "full_db"),
					resource.TestCheckResourceAttr("iceberg_table.full", "user_properties.owner", "terraform"),
					resource.TestCheckResourceAttr("iceberg_table.full", "user_properties.env", "test"),

					// Check schema fields
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.0.name", "id"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.0.id", "1"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.0.type", "int"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.0.required", "true"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.0.doc", "The unique identifier"),

					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.1.name", "data"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.1.id", "2"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.1.type", "string"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.1.required", "false"),

					// Check decimal and fixed
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.2.name", "price"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.2.id", "3"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.2.type", "decimal(10,2)"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.3.name", "hash"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.3.id", "4"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.3.type", "fixed[16]"),

					// Check nested struct
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.4.name", "location"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.4.id", "5"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.4.type", "struct"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.4.struct_properties.fields.0.name", "lat"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.4.struct_properties.fields.0.id", "8"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.4.struct_properties.fields.0.type", "double"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.4.struct_properties.fields.1.name", "long"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.4.struct_properties.fields.1.id", "9"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.4.struct_properties.fields.1.type", "double"),

					// Check list
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.5.name", "tags"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.5.id", "6"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.5.type", "list"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.5.list_properties.element_id", "10"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.5.list_properties.element_type", "string"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.5.list_properties.element_required", "true"),

					// Check map
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.6.name", "metadata"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.6.id", "7"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.6.type", "map"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.6.map_properties.key_id", "11"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.6.map_properties.key_type", "string"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.6.map_properties.value_id", "12"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.6.map_properties.value_type", "int"),
					resource.TestCheckResourceAttr("iceberg_table.full", "schema.fields.6.map_properties.value_required", "false"),
				),
			},
		},
	})
}

func testAccIcebergTableFullConfig(providerCfg string, tableName string) string {
	return providerCfg + fmt.Sprintf(`
resource "iceberg_namespace" "full_db" {
  name = ["full_db"]
}

resource "iceberg_table" "full" {
  namespace = iceberg_namespace.full_db.name
  name      = "%s"

  user_properties = {
    owner = "terraform"
    env   = "test"
  }

  schema = {
    fields = [
      {
        id       = 1
        name     = "id"
        type     = "int"
        required = true
        doc      = "The unique identifier"
      },
      {
        id       = 2
        name     = "data"
        type     = "string"
        required = false
      },
      {
        id       = 3
        name     = "price"
        type     = "decimal(10,2)"
        required = true
      },
      {
        id       = 4
        name     = "hash"
        type     = "fixed[16]"
        required = true
      },
      {
        id       = 5
        name     = "location"
        type     = "struct"
        required = true
        struct_properties = {
          fields = [
            {
              id       = 8
              name     = "lat"
              type     = "double"
              required = true
            },
            {
              id       = 9
              name     = "long"
              type     = "double"
              required = true
            }
          ]
        }
      },
      {
        id       = 6
        name     = "tags"
        type     = "list"
        required = false
        list_properties = {
          element_id       = 10
          element_type     = "string"
          element_required = true
        }
      },
      {
        id       = 7
        name     = "metadata"
        type     = "map"
        required = false
        map_properties = {
          key_id         = 11
          key_type       = "string"
          value_id       = 12
          value_type     = "int"
          value_required = false
        }
      }
    ]
  }
}
`, tableName)
}

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

provider "iceberg" {
  catalog_uri = "http://localhost:8181"
}

resource "iceberg_namespace" "example" {
  name        = ["example_namespace"]
  description = "An example namespace"
}

resource "iceberg_table" "example" {
  namespace = iceberg_namespace.example.name
  name      = "example_table"

  schema = {
    id = 0
    fields = [
      {
        id   = 0
        name = "id"
        type = {
          primitive = "long"
        }
        required = true
      },
      {
        id   = 1
        name = "data"
        type = {
          primitive = "string"
        }
        required = false
      },
      {
        id   = 2
        name = "tags"
        type = {
          list = {
            element_id       = 3
            element_type     = "string"
            element_required = true
          }
        }
        required = false
      }
    ]
  }
}

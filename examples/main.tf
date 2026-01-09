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
    id = 1
    fields = [
      {
        id   = 1
        name = "id"
        type = {
          primitive = "long"
        }
        required = true
      },
      {
        id   = 2
        name = "data"
        type = {
          primitive = "string"
        }
        required = false
      },
      {
        id   = 3
        name = "tags"
        type = {
          list = {
            element_id       = 4
            element_type     = "string"
            element_required = true
          }
        }
        required = false
      }
    ]
  }
}
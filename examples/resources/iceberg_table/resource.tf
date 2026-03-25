resource "iceberg_namespace" "example" {
  name = ["example_namespace"]
  user_properties = {
    description = "An example namespace"
  }
}

resource "iceberg_table" "example" {
  namespace = iceberg_namespace.example.name
  name      = "example_table"

  schema = {
    fields = [
      {
        name     = "id"
        type     = "long"
        required = true
      },
      {
        name     = "data"
        type     = "string"
        required = false
      },
      {
        name = "tags"
        type = "list"
        list_properties = {
          element_id       = 3
          element_type     = "string"
          element_required = true
        }
        required = false
      }
    ]
  }
}

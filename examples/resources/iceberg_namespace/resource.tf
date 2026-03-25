resource "iceberg_namespace" "example" {
  name = ["example_namespace"]
  user_properties = {
    description = "An example namespace"
  }
}

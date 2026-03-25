resource "iceberg_namespace" "example" {
  name = ["example_namespace"]
  user_properties = {
    "key" = "value"
  }
}

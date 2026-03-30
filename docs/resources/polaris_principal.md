---
page_title: "iceberg_polaris_principal Resource - Iceberg"
subcategory: ""
description: |-
  A resource for managing Polaris principals and their client credentials.
---

# iceberg_polaris_principal (Resource)

A resource for managing Polaris principals and their client credentials.



## Schema

### Required

- `name` (String) The name of the Polaris principal.

### Optional

- `credential_rotation_required` (Boolean) If true, the initial credentials can only be used to call rotateCredentials.
- `properties` (Map of String) Arbitrary metadata properties for the principal.

### Read-Only

- `client_id` (String, Sensitive) The client ID associated with this principal. Computed after create.
- `client_secret` (String, Sensitive) The client secret associated with this principal. Polaris only allows setting/resetting via resetCredentials after create; this provider stores the secret after create and preserves it on update.
- `id` (String) The ID of this resource.

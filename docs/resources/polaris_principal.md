---
page_title: "iceberg_polaris_principal Resource - Iceberg"
subcategory: ""
description: |-
  A resource for managing Polaris principals and their client credentials.
---

<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

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

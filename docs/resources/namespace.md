---
page_title: "iceberg_namespace Resource - Iceberg"
subcategory: ""
description: |-
  A resource for managing Iceberg namespaces.
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

# iceberg_namespace (Resource)

A resource for managing Iceberg namespaces.

## Example Usage

```terraform
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

resource "iceberg_namespace" "example" {
  name = ["example_namespace"]
  user_properties = {
    description = "An example namespace"
  }
}
```

## Schema

### Required

- `name` (List of String) The name of the namespace.

### Optional

- `user_properties` (Map of String) User-defined properties for the namespace. Only properties listed in Terraform will be changed. All others on the server will stay the same

### Read-Only

- `id` (String) The ID of this resource.
- `server_properties` (Map of String) Full properties returned by the server for the namespace. This includes properties set by the user and properties set by the server.

## Import

Import is supported using the following syntax:

```shell
$ terraform import iceberg_namespace a.b.c
```

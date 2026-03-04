#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Obtain an OAuth access token from a Polaris token endpoint for local dev.
Prints the access_token to stdout so it can be used in Make or shell (e.g. POLARIS_TOKEN=$(python3 dev/provision_polaris.py)).
"""

import argparse
import json
import sys
import urllib.parse
import urllib.request


def main() -> int:
    parser = argparse.ArgumentParser(description="Get Polaris OAuth access token for dev.")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8191",
        help="Polaris base URL (default: http://localhost:8191)",
    )
    parser.add_argument(
        "--user",
        default="root:s3cr3t",
        help="Basic auth user:password for token endpoint (default: root:s3cr3t)",
    )
    parser.add_argument(
        "--scope",
        default="PRINCIPAL_ROLE:ALL",
        help="OAuth scope (default: PRINCIPAL_ROLE:ALL)",
    )
    args = parser.parse_args()

    url = urllib.parse.urljoin(args.base_url.rstrip("/") + "/", "api/catalog/v1/oauth/tokens")
    data = urllib.parse.urlencode(
        {"grant_type": "client_credentials", "scope": args.scope}
    ).encode()

    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    user, _, password = args.user.partition(":")
    if not password:
        print("provision_polaris: --user must be user:password", file=sys.stderr)
        return 1
    import base64

    req.add_header(
        "Authorization",
        "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode(),
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.load(resp)
    except urllib.error.URLError as e:
        print(f"provision_polaris: {e}", file=sys.stderr)
        return 1
    except json.JSONDecodeError as e:
        print(f"provision_polaris: invalid JSON: {e}", file=sys.stderr)
        return 1

    token = body.get("access_token")
    if not token:
        print("provision_polaris: response missing access_token", file=sys.stderr)
        return 1
    print(token)
    return 0


if __name__ == "__main__":
    sys.exit(main())

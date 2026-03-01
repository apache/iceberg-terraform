// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
)

type polarisClient struct {
	baseURL    *url.URL
	httpClient *http.Client
	token      string
	headers    map[string]string
}

type polarisNotFoundError struct {
	method string
	path   string
}

func (e *polarisNotFoundError) Error() string {
	return fmt.Sprintf("polaris: not found %s %s", e.method, e.path)
}

func isPolarisNotFoundError(err error) bool {
	var nf *polarisNotFoundError
	return errors.As(err, &nf)
}

func (p *icebergProvider) newPolarisClient() (*polarisClient, error) {
	if p.polarisManagementURI == "" {
		return nil, fmt.Errorf("polaris_management_uri is not configured and could not be derived from catalog_uri")
	}
	u, err := url.Parse(p.polarisManagementURI)
	if err != nil {
		return nil, fmt.Errorf("invalid polaris_management_uri %q: %w", p.polarisManagementURI, err)
	}

	return &polarisClient{
		baseURL:    u,
		httpClient: http.DefaultClient,
		token:      p.token,
		headers:    p.headers,
	}, nil
}

func (c *polarisClient) do(ctx context.Context, method, relativePath string, query url.Values, body any, out any) error {
	u := *c.baseURL

	u.Path = path.Join(c.baseURL.Path, relativePath)
	u.RawQuery = query.Encode()

	var reqBody io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request body: %w", err)
		}
		reqBody = bytes.NewReader(buf)
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), reqBody)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	for k, v := range c.headers {
		// don't override existing headers if users are also setting it
		if _, exists := req.Header[k]; !exists {
			req.Header.Set(k, v)
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("perform request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return &polarisNotFoundError{method: method, path: u.Path}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("polaris: unexpected status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if out == nil {
		return nil
	}

	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(out); err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}

type polarisPrincipal struct {
	Name                string            `json:"name"`
	Properties          map[string]string `json:"properties,omitempty"`
	EntityVersion       int64             `json:"entityVersion,omitempty"`
	ClientID            string            `json:"clientId,omitempty"`
	CreateTimestamp     int64             `json:"createTimestamp,omitempty"`
	LastUpdateTimestamp int64             `json:"lastUpdateTimestamp,omitempty"`
}

type polarisPrincipalWithCredentials struct {
	Principal   polarisPrincipal `json:"principal"`
	Credentials struct {
		ClientID     string `json:"clientId"`
		ClientSecret string `json:"clientSecret"`
	} `json:"credentials"`
}

type polarisCreatePrincipalRequest struct {
	Principal                  polarisPrincipal `json:"principal"`
	CredentialRotationRequired *bool            `json:"credentialRotationRequired,omitempty"`
}

type polarisUpdatePrincipalRequest struct {
	CurrentEntityVersion int64             `json:"currentEntityVersion"`
	Properties           map[string]string `json:"properties,omitempty"`
}

func (c *polarisClient) CreatePrincipal(ctx context.Context, req polarisCreatePrincipalRequest) (*polarisPrincipalWithCredentials, error) {
	var out polarisPrincipalWithCredentials
	if err := c.do(ctx, http.MethodPost, "/principals", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *polarisClient) GetPrincipal(ctx context.Context, name string) (*polarisPrincipal, error) {
	var out polarisPrincipal
	if err := c.do(ctx, http.MethodGet, "/principals/"+url.PathEscape(name), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *polarisClient) UpdatePrincipal(ctx context.Context, name string, req polarisUpdatePrincipalRequest) (*polarisPrincipal, error) {
	var out polarisPrincipal
	if err := c.do(ctx, http.MethodPut, "/principals/"+url.PathEscape(name), nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *polarisClient) DeletePrincipal(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/principals/"+url.PathEscape(name), nil, nil, nil)
}

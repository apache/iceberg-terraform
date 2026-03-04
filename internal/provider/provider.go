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

package provider

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ provider.Provider = &icebergProvider{}
)

// New is a helper function to simplify provider server and testing implementation.
func New() func() provider.Provider {
	return func() provider.Provider {
		return &icebergProvider{}
	}
}

// polarisConfig holds Polaris-specific provider settings. Used only when type is "polaris".
type polarisConfig struct {
	managementURI string
	catalogName   string
}

// icebergProvider is the provider implementation.
type icebergProvider struct {
	catalogURI  string
	catalogType string // passed to iceberg-go as "rest" when type is "polaris"
	token       string
	warehouse   string
	headers     map[string]string
	polaris     *polarisConfig // non-nil when type is "polaris"
}

// icebergProviderModel maps provider schema data to a Go type.
type icebergProviderModel struct {
	CatalogURI           types.String `tfsdk:"catalog_uri"`
	Type                 types.String `tfsdk:"type"`
	Token                types.String `tfsdk:"token"`
	Warehouse            types.String `tfsdk:"warehouse"`
	Headers              types.Map    `tfsdk:"headers"`
	PolarisManagementURI types.String `tfsdk:"polaris_management_uri"`
	PolarisCatalogName   types.String `tfsdk:"polaris_catalog_name"`
}

// Metadata returns the provider type name.
func (p *icebergProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "iceberg"
}

// Schema defines the provider-level schema for configuration data.
func (p *icebergProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Use Terraform to interact with Iceberg REST Catalog instances.",
		Attributes: map[string]schema.Attribute{
			"catalog_uri": schema.StringAttribute{
				Description: "The URI of the Iceberg REST catalog.",
				Required:    true,
			},
			"type": schema.StringAttribute{
				Description: "The type of catalog. Use 'rest' for a plain REST catalog, or 'polaris' for Polaris (REST catalog with Polaris management). When 'polaris', polaris_management_uri and polaris_catalog_name may be set.",
				Optional:    true,
			},
			"token": schema.StringAttribute{
				Description: "The token to use for authentication.",
				Optional:    true,
				Sensitive:   true,
			},
			"warehouse": schema.StringAttribute{
				Description: "The warehouse to use for the Iceberg REST catalog. This will be passed as `warehouse` property in the catalog properties.",
				Optional:    true,
			},
			"headers": schema.MapAttribute{
				Description: "The headers to use for authentication.",
				Optional:    true,
				Sensitive:   true,
				ElementType: types.StringType,
			},
			"polaris_management_uri": schema.StringAttribute{
				Description: "The base URI for the Polaris Management API. If omitted, it will be derived from catalog_uri by replacing the path with '/api/management/v1'",
				Optional:    true,
			},
			"polaris_catalog_name": schema.StringAttribute{
				Description: "Optional Polaris catalog name to use as the default when managing Polaris RBAC resources",
				Optional:    true,
			},
		},
	}
}

// Configure prepares a Iceberg API client for data sources and resources.
func (p *icebergProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var data icebergProviderModel

	diags := req.Config.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	if data.CatalogURI.IsUnknown() {
		return
	}

	p.catalogURI = data.CatalogURI.ValueString()

	catalogType := "rest"
	if !data.Type.IsNull() && !data.Type.IsUnknown() {
		catalogType = data.Type.ValueString()
	}

	switch catalogType {
	case "rest":
		p.catalogType = "rest"
		p.polaris = nil
	case "polaris":
		p.catalogType = "rest"
		p.polaris = &polarisConfig{}
		if !data.PolarisCatalogName.IsNull() && !data.PolarisCatalogName.IsUnknown() {
			p.polaris.catalogName = data.PolarisCatalogName.ValueString()
		}
		if !data.PolarisManagementURI.IsNull() && !data.PolarisManagementURI.IsUnknown() {
			p.polaris.managementURI = strings.TrimRight(data.PolarisManagementURI.ValueString(), "/")
		} else if p.catalogURI != "" {
			if u, err := url.Parse(p.catalogURI); err == nil {
				u.Path = "/api/management/v1"
				p.polaris.managementURI = strings.TrimRight(u.String(), "/")
			}
		}
	default:
		resp.Diagnostics.AddError(
			"Unsupported Catalog Type",
			"The provider supports 'rest' and 'polaris'. Got: "+catalogType,
		)
		return
	}

	if !data.Token.IsNull() && !data.Token.IsUnknown() {
		p.token = data.Token.ValueString()
	}

	if !data.Warehouse.IsNull() && !data.Warehouse.IsUnknown() {
		p.warehouse = data.Warehouse.ValueString()
	}

	if !data.Headers.IsNull() && !data.Headers.IsUnknown() {
		headers := make(map[string]string)
		resp.Diagnostics.Append(data.Headers.ElementsAs(ctx, &headers, false)...)
		if resp.Diagnostics.HasError() {
			return
		}

		p.headers = headers
	}

	resp.DataSourceData = p
	resp.ResourceData = p
}

func (p *icebergProvider) NewCatalog(ctx context.Context) (catalog.Catalog, error) {
	opts := make([]rest.Option, 0)
	if p.token != "" {
		opts = append(opts, rest.WithOAuthToken(p.token))
	}

	if p.warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(p.warehouse))
	}

	opts = append(opts, rest.WithCustomTransport(&headerRoundTripper{headers: p.headers}))

	return rest.NewCatalog(ctx, p.catalogType, p.catalogURI, opts...)
}

type headerRoundTripper struct {
	headers map[string]string
}

func (h *headerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	for k, v := range h.headers {
		req.Header.Add(k, v)
	}
	return http.DefaultTransport.RoundTrip(req)
}

// DataSources defines the data sources implemented in the provider.
func (p *icebergProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return nil
}

// Resources defines the resources implemented in the provider.
func (p *icebergProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewNamespaceResource,
		NewTableResource,
		NewPolarisPrincipalResource,
	}
}

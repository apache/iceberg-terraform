package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
)

// Ensure the implementation satisfies the expected interfaces.
var (
	_ provider.Provider = &icebergProvider{}
)

// New is a helper function to simplify provider server and testing implementation.
func New() provider.Provider {
	return &icebergProvider{}
}

// icebergProvider is the provider implementation.
type icebergProvider struct{}

// Metadata returns the provider type name.
func (p *icebergProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "iceberg"
}

// Schema defines the provider-level schema for configuration data.
func (p *icebergProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Use Terraform to interact with Iceberg REST Catalog instances.",
	}
}

// Configure prepares a Iceberg API client for data sources and resources.
func (p *icebergProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	// Provider schema is empty, so no configuration to retrieve.
}

// DataSources defines the data sources implemented in the provider.
func (p *icebergProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return nil
}

// Resources defines the resources implemented in the provider.
func (p *icebergProvider) Resources(_ context.Context) []func() resource.Resource {
	return nil
}

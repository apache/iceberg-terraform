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
	"errors"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/listplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var (
	_ resource.Resource = &icebergNamespaceResource{}
)

func NewNamespaceResource() resource.Resource {
	return &icebergNamespaceResource{}
}

type icebergNamespaceResourceModel struct {
	ID               types.String `tfsdk:"id"`
	Name             types.List   `tfsdk:"name"`
	UserProperties   types.Map    `tfsdk:"user_properties"`
	ServerProperties types.Map    `tfsdk:"server_properties"`
}

type icebergNamespaceResource struct {
	catalog  catalog.Catalog
	provider *icebergProvider
}

func (r *icebergNamespaceResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_namespace"
}

func (r *icebergNamespaceResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "A resource for managing Iceberg namespaces.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.ListAttribute{
				Description: "The name of the namespace.",
				Required:    true,
				ElementType: types.StringType,
				PlanModifiers: []planmodifier.List{
					listplanmodifier.RequiresReplace(),
				},
			},
			"user_properties": schema.MapAttribute{
				Description: "User-defined properties for the namespace. Only properties listed in Terraform will be changed. All others on the server will stay the same",
				Optional:    true,
				ElementType: types.StringType,
			},
			"server_properties": schema.MapAttribute{
				Description: "Full properties returned by the server for the namespace. This includes properties set by the user and properties set by the server.",
				Computed:    true,
				ElementType: types.StringType,
			},
		},
	}
}

func (r *icebergNamespaceResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	provider, ok := req.ProviderData.(*icebergProvider)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			"Expected *icebergProvider, got: %T. Please report this issue to the provider developers.",
		)
		return
	}

	r.provider = provider
}

func (r *icebergNamespaceResource) ConfigureCatalog(ctx context.Context, diags *diag.Diagnostics) {
	if r.catalog != nil {
		return
	}

	if r.provider == nil {
		diags.AddError(
			"Provider not configured",
			"The provider hasn't been configured before this operation",
		)
		return
	}

	catalog, err := r.provider.NewCatalog(ctx)
	if err != nil {
		diags.AddError(
			"Failed to create catalog",
			"Failed to create catalog: "+err.Error(),
		)
		return
	}
	r.catalog = catalog
}

func (r *icebergNamespaceResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	r.ConfigureCatalog(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data icebergNamespaceResourceModel

	diags := req.Plan.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	var namespaceName []string
	diags = data.Name.ElementsAs(ctx, &namespaceName, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	namespaceIdent := catalog.ToIdentifier(namespaceName...)

	userProperties := make(map[string]string)
	if !data.UserProperties.IsNull() {
		diags = data.UserProperties.ElementsAs(ctx, &userProperties, false)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	err := r.catalog.CreateNamespace(ctx, namespaceIdent, userProperties)
	if err != nil {
		resp.Diagnostics.AddError("failed to create namespace", err.Error())
		return
	}

	data.ID = types.StringValue(strings.Join(namespaceIdent, "."))

	nsProps, err := r.catalog.LoadNamespaceProperties(ctx, namespaceIdent)
	if err != nil {
		resp.Diagnostics.AddError("failed to read namespace properties", err.Error())
		return
	}

	// Update ServerProperties with everything from the server
	loadedFullProperties, diags := types.MapValueFrom(ctx, types.StringType, nsProps)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	data.ServerProperties = loadedFullProperties

	// Update UserProperties to match what we sent/expected, but values confirmed from server
	// We only keep keys that were in the original plan (User managed)
	managedProps := make(map[string]string)
	for k := range userProperties {
		if v, ok := nsProps[k]; ok {
			managedProps[k] = v
		}
	}
	// If the user didn't set any properties, UserProperties should be null or empty based on input.
	// However, if we sent it, we expect it back.
	if !data.UserProperties.IsNull() {
		data.UserProperties, diags = types.MapValueFrom(ctx, types.StringType, managedProps)
		resp.Diagnostics.Append(diags...)
	}

	diags = resp.State.Set(ctx, &data)
	resp.Diagnostics.Append(diags...)
}

func (r *icebergNamespaceResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	r.ConfigureCatalog(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data icebergNamespaceResourceModel

	tflog.Info(ctx, "Reading namespace resource")
	diags := req.State.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	var namespaceName []string
	diags = data.Name.ElementsAs(ctx, &namespaceName, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	namespaceIdent := catalog.ToIdentifier(namespaceName...)

	nsProps, err := r.catalog.LoadNamespaceProperties(ctx, namespaceIdent)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("failed to load namespace", err.Error())
		return
	}

	// ServerProperties gets everything
	fullProperties, diags := types.MapValueFrom(ctx, types.StringType, nsProps)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	data.ServerProperties = fullProperties

	// UserProperties only updates keys that are already tracked in the state
	if !data.UserProperties.IsNull() {
		stateProperties := make(map[string]string)
		diags = data.UserProperties.ElementsAs(ctx, &stateProperties, false)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}

		managedProps := make(map[string]string)
		for k := range stateProperties {
			if v, ok := nsProps[k]; ok {
				managedProps[k] = v
			}
			// If key is missing in nsProps, it was removed from server, so we drop it from managedProps
			// which effectively sets it to null/removed in the new state, matching reality.
		}
		data.UserProperties, diags = types.MapValueFrom(ctx, types.StringType, managedProps)
		resp.Diagnostics.Append(diags...)
	}

	diags = resp.State.Set(ctx, &data)
	resp.Diagnostics.Append(diags...)
}

func (r *icebergNamespaceResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	r.ConfigureCatalog(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var plan, state icebergNamespaceResourceModel

	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)

	diags = req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	updates := make(iceberg.Properties)
	removals := make([]string, 0)

	// Get current state properties
	stateProps := make(map[string]string)
	if !state.UserProperties.IsNull() {
		diags = state.UserProperties.ElementsAs(ctx, &stateProps, false)
		resp.Diagnostics.Append(diags...)
	}

	// Get plan properties
	planProps := make(map[string]string)
	if !plan.UserProperties.IsNull() {
		diags = plan.UserProperties.ElementsAs(ctx, &planProps, false)
		resp.Diagnostics.Append(diags...)
	}

	if resp.Diagnostics.HasError() {
		return
	}

	// Calculate updates: keys in plan that differ from state
	for k, v := range planProps {
		if oldV, ok := stateProps[k]; !ok || oldV != v {
			updates[k] = v
		}
	}

	// Calculate removals: keys in state that are NOT in plan
	for k := range stateProps {
		if _, ok := planProps[k]; !ok {
			removals = append(removals, k)
		}
	}

	if len(updates) == 0 && len(removals) == 0 {
		return
	}

	var namespaceName []string
	diags = plan.Name.ElementsAs(ctx, &namespaceName, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	namespaceIdent := catalog.ToIdentifier(namespaceName...)

	_, err := r.catalog.UpdateNamespaceProperties(ctx, namespaceIdent, removals, updates)
	if err != nil {
		resp.Diagnostics.AddError("failed to update namespace properties", err.Error())
		return
	}

	nsProps, err := r.catalog.LoadNamespaceProperties(ctx, namespaceIdent)
	if err != nil {
		resp.Diagnostics.AddError("failed to read namespace properties", err.Error())
		return
	}

	// Update ServerProperties
	loadedFullProperties, diags := types.MapValueFrom(ctx, types.StringType, nsProps)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	plan.ServerProperties = loadedFullProperties

	// Update UserProperties to match reality for tracked keys
	// We reconstruct plan.UserProperties to ensure it reflects what's actually on the server for the keys we care about
	managedProps := make(map[string]string)
	for k := range planProps {
		if v, ok := nsProps[k]; ok {
			managedProps[k] = v
		}
	}
	if !plan.UserProperties.IsNull() {
		plan.UserProperties, diags = types.MapValueFrom(ctx, types.StringType, managedProps)
		resp.Diagnostics.Append(diags...)
	}

	diags = resp.State.Set(ctx, &plan)
	resp.Diagnostics.Append(diags...)
}

func (r *icebergNamespaceResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	r.ConfigureCatalog(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data icebergNamespaceResourceModel

	diags := req.State.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	var namespaceName []string
	diags = data.Name.ElementsAs(ctx, &namespaceName, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	namespaceIdent := catalog.ToIdentifier(namespaceName...)

	err := r.catalog.DropNamespace(ctx, namespaceIdent)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			// If the namespace is already gone, we don't need to do anything.
			return
		}
		resp.Diagnostics.AddError("failed to drop namespace", err.Error())
		return
	}
}

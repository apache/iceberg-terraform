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

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var (
	_ resource.Resource                = &polarisPrincipalRoleResource{}
	_ resource.ResourceWithImportState = &polarisPrincipalRoleResource{}
)

func NewPolarisPrincipalRoleResource() resource.Resource {
	return &polarisPrincipalRoleResource{}
}

type polarisPrincipalRoleResource struct {
	provider         *icebergProvider
	managementClient *polarisManagementClient
}

type polarisPrincipalRoleResourceModel struct {
	ID         types.String `tfsdk:"id"`
	Name       types.String `tfsdk:"name"`
	Federated  types.Bool   `tfsdk:"federated"`
	Properties types.Map    `tfsdk:"properties"`
}

func (r *polarisPrincipalRoleResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_polaris_principal_role"
}

func (r *polarisPrincipalRoleResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Manages a Polaris principal role (management API).",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				Description: "The principal role name.",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"federated": schema.BoolAttribute{
				Description: "Whether the role is federated (managed by an external IdP). Updates are not supported by the API; changing this forces replacement.",
				Optional:    true,
				Computed:    true,
				Default:     booldefault.StaticBool(false),
				PlanModifiers: []planmodifier.Bool{
					boolplanmodifier.RequiresReplace(),
				},
			},
			"properties": schema.MapAttribute{
				Description: "Metadata properties for the principal role.",
				Optional:    true,
				ElementType: types.StringType,
			},
		},
	}
}

func (r *polarisPrincipalRoleResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	provider, ok := req.ProviderData.(*icebergProvider)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			"Expected *icebergProvider, got a different type: %T. Please report this issue to the provider developers.",
		)
	}
	r.provider = provider
}

func (r *polarisPrincipalRoleResource) ensureManagementClient(ctx context.Context, diags *diag.Diagnostics) {
	if r.managementClient != nil {
		return
	}
	if r.provider == nil {
		diags.AddError(
			"Provider not configured",
			"The provider hasn't been configured before this operation")

		return
	}
	client, err := r.provider.newPolarisManagementClient()
	if err != nil {
		diags.AddError("Failed to create Polaris management API client", err.Error())

		return
	}
	r.managementClient = client
}

func (r *polarisPrincipalRoleResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	r.ensureManagementClient(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data polarisPrincipalRoleResourceModel

	diags := req.Plan.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := data.Name.ValueString()

	props := make(map[string]string)
	if !data.Properties.IsNull() && !data.Properties.IsUnknown() {
		diags = data.Properties.ElementsAs(ctx, &props, false)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	pr := polarisPrincipalRole{
		Name:       name,
		Properties: props,
	}
	if !data.Federated.IsNull() && !data.Federated.IsUnknown() {
		v := data.Federated.ValueBool()
		pr.Federated = &v
	}

	tflog.Info(ctx, "Creating Polaris principal role", map[string]any{"name": name})

	created, err := r.managementClient.CreatePrincipalRole(ctx, polarisCreatePrincipalRoleRequest{PrincipalRole: pr})
	if err != nil {
		resp.Diagnostics.AddError("failed to create principal role", err.Error())

		return
	}

	data.ID = types.StringValue(created.Name)
	data.Name = types.StringValue(created.Name)
	data.Federated = types.BoolValue(created.Federated != nil && *created.Federated)

	if len(created.Properties) > 0 {
		propsVal, diags := types.MapValueFrom(ctx, types.StringType, created.Properties)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		data.Properties = propsVal
	} else {
		data.Properties = types.MapNull(types.StringType)
	}

	diags = resp.State.Set(ctx, &data)
	resp.Diagnostics.Append(diags...)
}

func (r *polarisPrincipalRoleResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	r.ensureManagementClient(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data polarisPrincipalRoleResourceModel

	diags := req.State.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := data.Name.ValueString()

	tflog.Info(ctx, "Reading Polaris principal role", map[string]any{"name": name})

	role, err := r.managementClient.GetPrincipalRole(ctx, name)
	if err != nil {
		if isPolarisNotFoundError(err) {
			resp.State.RemoveResource(ctx)

			return
		}
		resp.Diagnostics.AddError("Failed to read Polaris principal role", err.Error())

		return
	}

	data.ID = types.StringValue(role.Name)
	data.Name = types.StringValue(role.Name)
	data.Federated = types.BoolValue(role.Federated != nil && *role.Federated)

	if len(role.Properties) > 0 {
		propsVal, diags := types.MapValueFrom(ctx, types.StringType, role.Properties)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		data.Properties = propsVal
	} else {
		data.Properties = types.MapNull(types.StringType)
	}

	diags = resp.State.Set(ctx, &data)
	resp.Diagnostics.Append(diags...)
}

func (r *polarisPrincipalRoleResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	r.ensureManagementClient(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var plan, state polarisPrincipalRoleResourceModel

	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)

	diags = req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	name := state.Name.ValueString()

	current, err := r.managementClient.GetPrincipalRole(ctx, name)
	if err != nil {
		var nf *polarisNotFoundError
		if errors.As(err, &nf) {
			resp.State.RemoveResource(ctx)

			return
		}
		resp.Diagnostics.AddError("Failed to read Polaris principal role for update", err.Error())

		return
	}

	props := make(map[string]string)
	if !plan.Properties.IsNull() && !plan.Properties.IsUnknown() {
		diags = plan.Properties.ElementsAs(ctx, &props, false)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	updateReq := polarisUpdatePrincipalRoleRequest{
		CurrentEntityVersion: current.EntityVersion,
		Properties:           props,
	}

	tflog.Info(ctx, "Updating Polaris principal role", map[string]any{"name": name})

	updated, err := r.managementClient.UpdatePrincipalRole(ctx, name, updateReq)
	if err != nil {
		var nf *polarisNotFoundError
		if errors.As(err, &nf) {
			resp.State.RemoveResource(ctx)

			return
		}
		resp.Diagnostics.AddError("Failed to update Polaris principal role", err.Error())

		return
	}

	state.ID = types.StringValue(updated.Name)
	state.Name = types.StringValue(updated.Name)
	state.Federated = types.BoolValue(updated.Federated != nil && *updated.Federated)

	if len(updated.Properties) > 0 {
		propsVal, diags := types.MapValueFrom(ctx, types.StringType, updated.Properties)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}
		state.Properties = propsVal
	} else {
		state.Properties = types.MapNull(types.StringType)
	}

	diags = resp.State.Set(ctx, &state)
	resp.Diagnostics.Append(diags...)
}

func (r *polarisPrincipalRoleResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	r.ensureManagementClient(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data polarisPrincipalRoleResourceModel

	diags := req.State.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	name := data.Name.ValueString()

	tflog.Info(ctx, "Deleting Polaris principal role", map[string]any{"name": name})

	err := r.managementClient.DeletePrincipalRole(ctx, name)
	if err != nil && !isPolarisNotFoundError(err) {
		resp.Diagnostics.AddError("Failed to delete Polaris principal role", err.Error())

		return
	}
}

func (r *polarisPrincipalRoleResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), req.ID)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("name"), req.ID)...)
}

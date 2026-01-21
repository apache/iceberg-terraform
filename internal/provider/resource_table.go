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
	rscschema "github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/objectplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var (
	_ resource.Resource = &icebergTableResource{}
)

func NewTableResource() resource.Resource {
	return &icebergTableResource{}
}


type icebergTableResourceModel struct {
	ID             types.String `tfsdk:"id"`
	Namespace      types.List   `tfsdk:"namespace"`
	Name           types.String `tfsdk:"name"`
	Schema         types.Object `tfsdk:"schema"`
	FullProperties types.Map    `tfsdk:"full_properties"`
}

type icebergTableResource struct {
	catalog  catalog.Catalog
	provider *icebergProvider
}

func (r *icebergTableResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_table"
}

func (r *icebergTableResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = rscschema.Schema{
		Description: "A resource for managing Iceberg tables.",
		Attributes: map[string]rscschema.Attribute{
			"id": rscschema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"namespace": rscschema.ListAttribute{
				Description: "The namespace of the table.",
				Required:    true,
				ElementType: types.StringType,
			},
			"name": rscschema.StringAttribute{
				Description: "The name of the table.",
				Required:    true,
			},
			"schema": rscschema.SingleNestedAttribute{
				Description: "The schema of the table.",
				Required:    true,
				// TODO: Update schema in place instead of replacement
				PlanModifiers: []planmodifier.Object{
					objectplanmodifier.RequiresReplace(),
				},
				Attributes: map[string]rscschema.Attribute{
					"id": rscschema.Int64Attribute{
						Description: "The schema ID.",
						Required:    true,
					},
					"fields": rscschema.ListNestedAttribute{
						Description: "The fields of the schema.",
						Required:    true,
						NestedObject: rscschema.NestedAttributeObject{
							Attributes: map[string]rscschema.Attribute{
								"id": rscschema.Int64Attribute{
									Description: "The field ID.",
									Required:    true,
								},
								"name": rscschema.StringAttribute{
									Description: "The field name.",
									Required:    true,
								},
								"type": rscschema.SingleNestedAttribute{
									Description: "The field type.",
									Required:    true,
									Attributes: map[string]rscschema.Attribute{
										"primitive": rscschema.StringAttribute{
											Description: "The primitive type.",
											Optional:    true,
										},
										"list": rscschema.SingleNestedAttribute{
											Description: "The list type.",
											Optional:    true,
											Attributes: map[string]rscschema.Attribute{
												"element_id": rscschema.Int64Attribute{
													Description: "The list element id.",
													Required:    true,
												},
												"element_type": rscschema.StringAttribute{
													Description: "The list element type.",
													Required:    true,
												},
												"element_required": rscschema.BoolAttribute{
													Description: "Whether the list element is required.",
													Required:    true,
												},
											},
										},
										"map": rscschema.SingleNestedAttribute{
											Description: "The map type.",
											Optional:    true,
											Attributes: map[string]rscschema.Attribute{
												"key_id": rscschema.Int64Attribute{
													Description: "The map key id.",
													Required:    true,
												},
												"key_type": rscschema.StringAttribute{
													Description: "The map key type.",
													Required:    true,
												},
												"value_id": rscschema.Int64Attribute{
													Description: "The map value id.",
													Required:    true,
												},
												"value_type": rscschema.StringAttribute{
													Description: "The map value type.",
													Required:    true,
												},
												"value_required": rscschema.BoolAttribute{
													Description: "Whether the map value is required.",
													Required:    true,
												},
											},
										},
									},
								},
								"required": rscschema.BoolAttribute{
									Description: "Whether the field is required.",
									Required:    true,
								},
								"doc": rscschema.StringAttribute{
									Description: "The field documentation.",
									Optional:    true,
								},
							},
						},
					},
				},
			},
			"full_properties": rscschema.MapAttribute{
				Description: "Full properties returned by IRC for the table. Cannot be set by users.",
				Computed:    true,
				ElementType: types.StringType,
			},
		},
	}
}

func (r *icebergTableResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *icebergTableResource) ConfigureCatalog(ctx context.Context, diags *diag.Diagnostics) {
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

func (r *icebergTableResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	r.ConfigureCatalog(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data icebergTableResourceModel

	diags := req.Plan.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	var namespaceName []string
	diags = data.Namespace.ElementsAs(ctx, &namespaceName, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	tableName := data.Name.ValueString()
	tableIdent := catalog.ToIdentifier(append(namespaceName, tableName)...)

	var schema icebergTableSchema
	diags = data.Schema.As(ctx, &schema, basetypes.ObjectAsOptions{})
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	fields := make([]iceberg.NestedField, len(schema.Fields))
	for i, fieldObj := range schema.Fields {
		var field icebergTableSchemaField
		diags = fieldObj.As(ctx, &field, basetypes.ObjectAsOptions{})
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
			return
		}

		typ, err := terraformTypeToIcebergType(field.Type)
		if err != nil {
			resp.Diagnostics.AddError("invalid field type", err.Error())
			return
		}

		fields[i] = iceberg.NestedField{
			ID:       int(field.ID.ValueInt64()),
			Name:     field.Name.ValueString(),
			Type:     typ,
			Required: field.Required.ValueBool(),
			Doc:      field.Doc.ValueString(),
		}
	}

	tblSchema := iceberg.NewSchema(int(schema.ID.ValueInt64()), fields...)

	// TODO: Add PartitionSpec support
	tbl, err := r.catalog.CreateTable(ctx, tableIdent, tblSchema)
	if err != nil {
		resp.Diagnostics.AddError("failed to create table", err.Error())
		return
	}

	data.ID = types.StringValue(strings.Join(tableIdent, "."))

	loadedProperties, diags := types.MapValueFrom(ctx, types.StringType, tbl.Properties())
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	data.FullProperties = loadedProperties

	diags = resp.State.Set(ctx, &data)
	resp.Diagnostics.Append(diags...)
}

func (r *icebergTableResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	r.ConfigureCatalog(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data icebergTableResourceModel

	tflog.Info(ctx, "Reading table resource")
	diags := req.State.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	var namespaceName []string
	diags = data.Namespace.ElementsAs(ctx, &namespaceName, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	tableName := data.Name.ValueString()
	tableIdent := catalog.ToIdentifier(append(namespaceName, tableName)...)

	tbl, err := r.catalog.LoadTable(ctx, tableIdent)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("failed to load table", err.Error())
		return
	}

	fullProperties, diags := types.MapValueFrom(ctx, types.StringType, tbl.Properties())
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}
	data.FullProperties = fullProperties

	icebergSchema := tbl.Schema()
	fields := make([]attr.Value, len(icebergSchema.Fields()))
	for i, field := range icebergSchema.Fields() {
		terraformType, err := icebergTypeToTerraformType(field.Type)
		if err != nil {
			resp.Diagnostics.AddError("failed to convert iceberg type to terraform type", err.Error())
			return
		}

		fields[i] = types.ObjectValueMust(
			icebergTableSchemaField{}.AttrTypes(),
			map[string]attr.Value{
				"id":       types.Int64Value(int64(field.ID)),
				"name":     types.StringValue(field.Name),
				"type":     terraformType,
				"required": types.BoolValue(field.Required),
				"doc":      types.StringValue(field.Doc),
			},
		)
	}
	data.Schema = types.ObjectValueMust(
		icebergTableSchema{}.AttrTypes(),
		map[string]attr.Value{
			"id":     types.Int64Value(int64(icebergSchema.ID)),
			"fields": types.ListValueMust(types.ObjectType{AttrTypes: icebergTableSchemaField{}.AttrTypes()}, fields),
		},
	)

	diags = resp.State.Set(ctx, &data)
	resp.Diagnostics.Append(diags...)
}

func (r *icebergTableResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	r.ConfigureCatalog(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}
	// Not implemented yet
}

func (r *icebergTableResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	r.ConfigureCatalog(ctx, &resp.Diagnostics)
	if resp.Diagnostics.HasError() {
		return
	}

	var data icebergTableResourceModel

	diags := req.State.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	var namespaceName []string
	diags = data.Namespace.ElementsAs(ctx, &namespaceName, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	tableName := data.Name.ValueString()
	tableIdent := catalog.ToIdentifier(append(namespaceName, tableName)...)

	err := r.catalog.DropTable(ctx, tableIdent)
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			// If the table is already gone, we don't need to do anything.
			return
		}
		resp.Diagnostics.AddError("failed to drop table", err.Error())
		return
	}
}
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
	"github.com/apache/iceberg-go/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	rscschema "github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
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
	catalog catalog.Catalog
}

func (r *icebergTableResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_table"
}

func (r *icebergTableResource) Schema(_ context.Context, _ rscschema.SchemaRequest, resp *rscschema.SchemaResponse) {
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

	r.catalog = provider.catalog
}

type icebergTableSchema struct {
	ID     types.Int64  `tfsdk:"id"`
	Fields []types.Object `tfsdk:"fields"`
}

type icebergTableSchemaField struct {
	ID       types.Int64                   `tfsdk:"id"`
	Name     types.String                  `tfsdk:"name"`
	Type     icebergTableSchemaFieldType `tfsdk:"type"`
	Required types.Bool                    `tfsdk:"required"`
	Doc      types.String                  `tfsdk:"doc"`
}

type icebergTableSchemaFieldType struct {
	Primitive types.String `tfsdk:"primitive"`
	List      types.Object   `tfsdk:"list"`
	Map       types.Object   `tfsdk:"map"`
}

type icebergTableSchemaFieldTypeList struct {
	ElementID       types.Int64  `tfsdk:"element_id"`
	ElementType     types.String `tfsdk:"element_type"`
	ElementRequired types.Bool   `tfsdk:"element_required"`
}

type icebergTableSchemaFieldTypeMap struct {
	KeyID         types.Int64  `tfsdk:"key_id"`
	KeyType       types.String `tfsdk:"key_type"`
	ValueID       types.Int64  `tfsdk:"value_id"`
	ValueType     types.String `tfsdk:"value_type"`
	ValueRequired types.Bool   `tfsdk:"value_required"`
}

func (r *icebergTableResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
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
	diags = data.Schema.As(ctx, &schema, false)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	fields := make([]iceberg.NestedField, len(schema.Fields))
	for i, fieldObj := range schema.Fields {
		var field icebergTableSchemaField
		diags = fieldObj.As(ctx, &field, false)
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

	tbl, err := r.catalog.CreateTable(ctx, tableIdent, *tblSchema, iceberg.UnpartitionedSpec(), nil, nil)
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

func terraformTypeToIcebergType(typ icebergTableSchemaFieldType) (iceberg.Type, error) {
	if !typ.Primitive.IsNull() {
		return stringToType(typ.Primitive.ValueString())
	}

	if !typ.List.IsNull() {
		var list icebergTableSchemaFieldTypeList
		if err := typ.List.As(context.Background(), &list, false); err.HasError() {
			return nil, errors.New("failed to parse list type")
		}

		elemType, err := stringToType(list.ElementType.ValueString())
		if err != nil {
			return nil, err
		}

		return iceberg.ListType{
			ElementID:       int(list.ElementID.ValueInt64()),
			ElementType:     elemType,
			ElementRequired: list.ElementRequired.ValueBool(),
		}, nil
	}

	if !typ.Map.IsNull() {
		var m icebergTableSchemaFieldTypeMap
		if err := typ.Map.As(context.Background(), &m, false); err.HasError() {
			return nil, errors.New("failed to parse map type")
		}

		keyType, err := stringToType(m.KeyType.ValueString())
		if err != nil {
			return nil, err
		}

		valueType, err := stringToType(m.ValueType.ValueString())
		if err != nil {
			return nil, err
		}

		return iceberg.MapType{
			KeyID:         int(m.KeyID.ValueInt64()),
			KeyType:       keyType,
			ValueID:       int(m.ValueID.ValueInt64()),
			ValueType:     valueType,
			ValueRequired: m.ValueRequired.ValueBool(),
		}, nil
	}

	return nil, errors.New("unsupported type")
}

func stringToType(s string) (iceberg.Type, error) {
	switch s {
	case "boolean":
		return iceberg.BooleanType{}, nil
	case "int":
		return iceberg.Int32Type{}, nil
	case "long":
		return iceberg.Int64Type{}, nil
	case "float":
		return iceberg.Float32Type{}, nil
	case "double":
		return iceberg.Float64Type{}, nil
	case "decimal":
		return iceberg.DecimalType{}, nil
	case "date":
		return iceberg.DateType{}, nil
	case "time":
		return iceberg.TimeType{}, nil
	case "timestamp":
		return iceberg.TimestampType{}, nil
	case "timestamptz":
		return iceberg.TimestampTzType{}, nil
	case "string":
		return iceberg.StringType{}, nil
	case "uuid":
		return iceberg.UUIDType{}, nil
	case "fixed":
		return iceberg.FixedType{}, nil
	case "binary":
		return iceberg.BinaryType{}, nil
	}

	return nil, errors.New("unsupported type: " + s)
}

func (r *icebergTableResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
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

	data.FullProperties = fullProperties

	icebergSchema := tbl.Schema()
	fields := make([]attr.Value, len(icebergSchema.Fields()))
	for i, field := range icebergSchema.Fields() {
		terraformType, diags := icebergTypeToTerraformType(field.Type)
		resp.Diagnostics.Append(diags...)
		if resp.Diagnostics.HasError() {
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
		rscschema.SingleNestedAttribute{}.Attributes["schema"].GetType(),
		map[string]attr.Value{
			"id":     types.Int64Value(int64(icebergSchema.ID)),
			"fields": types.ListValueMust(icebergTableSchemaField{}.AttrTypes(), fields),
		},
	)
	diags = resp.State.Set(ctx, &data)
	resp.Diagnostics.Append(diags...)
}

func (r *icebergTableResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	// Not implemented yet
}

func (r *icebergTableResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
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
	tableIdent := catalog.toIdentifier(append(namespaceName, tableName)...)

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

func icebergTypeToTerraformType(t iceberg.Type) (attr.Value, diag.Diagnostics) {
	var diags diag.Diagnostics
	switch typ := t.(type) {
	case iceberg.BooleanType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("boolean"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.Int32Type:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("int"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.Int64Type:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("long"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.Float32Type:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("float"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.Float64Type:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("double"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.DecimalType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("decimal"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.DateType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("date"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.TimeType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("time"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.TimestampType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("timestamp"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.TimestampTzType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("timestamptz"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.StringType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("string"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.UUIDType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("uuid"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.FixedType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("fixed"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.BinaryType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("binary"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.ListType:
		elementType, elementDiags := icebergTypeToTerraformType(typ.ElementType)
		diags.Append(elementDiags...)
		if diags.HasError() {
			return types.ObjectNull(icebergTableSchemaFieldType{}.AttrTypes()), diags
		}
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringNull(),
				"list": types.ObjectValueMust(
					icebergTableSchemaFieldTypeList{}.AttrTypes(),
					map[string]attr.Value{
						"element_id":       types.Int64Value(int64(typ.ElementID)),
						"element_type":     elementType,
						"element_required": types.BoolValue(typ.ElementRequired),
					},
				),
				"map": types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), diags
	case iceberg.MapType:
		keyType, keyDiags := icebergTypeToTerraformType(typ.KeyType)
		diags.Append(keyDiags...)
		if diags.HasError() {
			return types.ObjectNull(icebergTableSchemaFieldType{}.AttrTypes()), diags
		}
		valueType, valueDiags := icebergTypeToTerraformType(typ.ValueType)
		diags.Append(valueDiags...)
		if diags.HasError() {
			return types.ObjectNull(icebergTableSchemaFieldType{}.AttrTypes()), diags
		}
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringNull(),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map": types.ObjectValueMust(
					icebergTableSchemaFieldTypeMap{}.AttrTypes(),
					map[string]attr.Value{
						"key_id":         types.Int64Value(int64(typ.KeyID)),
						"key_type":       keyType,
						"value_id":       types.Int64Value(int64(typ.ValueID)),
						"value_type":     valueType,
						"value_required": types.BoolValue(typ.ValueRequired),
					},
				),
			},
		), diags
	}
	diags.AddError("unsupported type", "Unsupported iceberg type: "+t.String())
	return types.ObjectNull(icebergTableSchemaFieldType{}.AttrTypes()), diags
}

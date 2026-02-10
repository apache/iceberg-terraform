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
	"regexp"
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
)

type icebergTableSchema struct {
	ID     types.Int64    `tfsdk:"id"`
	Fields []types.Object `tfsdk:"fields"`
}

func (icebergTableSchema) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"id": types.Int64Type,
		"fields": types.ListType{
			ElemType: types.ObjectType{
				AttrTypes: icebergTableSchemaField{}.AttrTypes(),
			},
		},
	}
}

type icebergTableSchemaField struct {
	ID               types.Int64  `tfsdk:"id"`
	Name             types.String `tfsdk:"name"`
	Type             types.String `tfsdk:"type"`
	Required         types.Bool   `tfsdk:"required"`
	Doc              types.String `tfsdk:"doc"`
	ListProperties   types.Object `tfsdk:"list_properties"`
	MapProperties    types.Object `tfsdk:"map_properties"`
	StructProperties types.Object `tfsdk:"struct_properties"`
}

func (icebergTableSchemaField) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"id":                types.Int64Type,
		"name":              types.StringType,
		"type":              types.StringType,
		"required":          types.BoolType,
		"doc":               types.StringType,
		"list_properties":   types.ObjectType{AttrTypes: icebergTableSchemaFieldListProperties{}.AttrTypes()},
		"map_properties":    types.ObjectType{AttrTypes: icebergTableSchemaFieldMapProperties{}.AttrTypes()},
		"struct_properties": types.ObjectType{AttrTypes: icebergTableSchemaFieldStructProperties{}.AttrTypes()},
	}
}

type icebergTableSchemaInnerField struct {
	ID               types.Int64  `tfsdk:"id"`
	Name             types.String `tfsdk:"name"`
	Type             types.String `tfsdk:"type"`
	Required         types.Bool   `tfsdk:"required"`
	Doc              types.String `tfsdk:"doc"`
	ListProperties   types.Object `tfsdk:"list_properties"`
	MapProperties    types.Object `tfsdk:"map_properties"`
	StructProperties types.Object `tfsdk:"struct_properties"`
}

func (icebergTableSchemaInnerField) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"id":                types.Int64Type,
		"name":              types.StringType,
		"type":              types.StringType,
		"required":          types.BoolType,
		"doc":               types.StringType,
		"list_properties":   types.ObjectType{AttrTypes: icebergTableSchemaFieldListProperties{}.AttrTypes()},
		"map_properties":    types.ObjectType{AttrTypes: icebergTableSchemaFieldMapProperties{}.AttrTypes()},
		"struct_properties": types.ObjectType{AttrTypes: icebergTableSchemaInnerStructProperties{}.AttrTypes()},
	}
}

type icebergTableSchemaLeafField struct {
	ID             types.Int64  `tfsdk:"id"`
	Name           types.String `tfsdk:"name"`
	Type           types.String `tfsdk:"type"`
	Required       types.Bool   `tfsdk:"required"`
	Doc            types.String `tfsdk:"doc"`
	ListProperties types.Object `tfsdk:"list_properties"`
	MapProperties  types.Object `tfsdk:"map_properties"`
	// No StructProperties to break recursion
}

func (icebergTableSchemaLeafField) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"id":              types.Int64Type,
		"name":            types.StringType,
		"type":            types.StringType,
		"required":        types.BoolType,
		"doc":             types.StringType,
		"list_properties": types.ObjectType{AttrTypes: icebergTableSchemaFieldListProperties{}.AttrTypes()},
		"map_properties":  types.ObjectType{AttrTypes: icebergTableSchemaFieldMapProperties{}.AttrTypes()},
	}
}

type icebergTableSchemaFieldListProperties struct {
	ElementID       types.Int64  `tfsdk:"element_id"`
	ElementType     types.String `tfsdk:"element_type"`
	ElementRequired types.Bool   `tfsdk:"element_required"`
}

func (icebergTableSchemaFieldListProperties) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"element_id":       types.Int64Type,
		"element_type":     types.StringType,
		"element_required": types.BoolType,
	}
}

type icebergTableSchemaFieldMapProperties struct {
	KeyID         types.Int64  `tfsdk:"key_id"`
	KeyType       types.String `tfsdk:"key_type"`
	ValueID       types.Int64  `tfsdk:"value_id"`
	ValueType     types.String `tfsdk:"value_type"`
	ValueRequired types.Bool   `tfsdk:"value_required"`
}

func (icebergTableSchemaFieldMapProperties) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"key_id":         types.Int64Type,
		"key_type":       types.StringType,
		"value_id":       types.Int64Type,
		"value_type":     types.StringType,
		"value_required": types.BoolType,
	}
}

type icebergTableSchemaFieldStructProperties struct {
	Fields types.List `tfsdk:"fields"`
}

func (icebergTableSchemaFieldStructProperties) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"fields": types.ListType{
			ElemType: types.ObjectType{
				AttrTypes: icebergTableSchemaInnerField{}.AttrTypes(),
			},
		},
	}
}

type icebergTableSchemaInnerStructProperties struct {
	Fields types.List `tfsdk:"fields"`
}

func (icebergTableSchemaInnerStructProperties) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"fields": types.ListType{
			ElemType: types.ObjectType{
				AttrTypes: icebergTableSchemaLeafField{}.AttrTypes(),
			},
		},
	}
}

func terraformToIcebergType(typeStr string, listProps, mapProps, structProps types.Object) (iceberg.Type, error) {
	switch typeStr {
	case "list":
		if listProps.IsNull() || listProps.IsUnknown() {
			return nil, errors.New("list_properties must be set for list type")
		}
		var props icebergTableSchemaFieldListProperties
		if err := listProps.As(context.Background(), &props, basetypes.ObjectAsOptions{}); err.HasError() {
			return nil, errors.New("failed to parse list_properties")
		}
		elemType, err := stringToType(props.ElementType.ValueString())
		if err != nil {
			return nil, err
		}
		return &iceberg.ListType{
			ElementID:       int(props.ElementID.ValueInt64()),
			Element:         elemType,
			ElementRequired: props.ElementRequired.ValueBool(),
		}, nil

	case "map":
		if mapProps.IsNull() || mapProps.IsUnknown() {
			return nil, errors.New("map_properties must be set for map type")
		}
		var props icebergTableSchemaFieldMapProperties
		if err := mapProps.As(context.Background(), &props, basetypes.ObjectAsOptions{}); err.HasError() {
			return nil, errors.New("failed to parse map_properties")
		}
		keyType, err := stringToType(props.KeyType.ValueString())
		if err != nil {
			return nil, err
		}
		valueType, err := stringToType(props.ValueType.ValueString())
		if err != nil {
			return nil, err
		}
		return &iceberg.MapType{
			KeyID:         int(props.KeyID.ValueInt64()),
			KeyType:       keyType,
			ValueID:       int(props.ValueID.ValueInt64()),
			ValueType:     valueType,
			ValueRequired: props.ValueRequired.ValueBool(),
		}, nil

	case "struct":
		return nil, errors.New("struct type handled by caller")
	}

	return stringToType(typeStr)
}

func convertIcebergTableSchemaField(field icebergTableSchemaField) (iceberg.Type, error) {
	if field.Type.ValueString() == "struct" {
		if field.StructProperties.IsNull() || field.StructProperties.IsUnknown() {
			return nil, errors.New("struct_properties must be set for struct type")
		}
		var structProps icebergTableSchemaFieldStructProperties
		if err := field.StructProperties.As(context.Background(), &structProps, basetypes.ObjectAsOptions{}); err.HasError() {
			return nil, errors.New("failed to parse struct_properties")
		}

		var innerFields []icebergTableSchemaInnerField
		if err := structProps.Fields.ElementsAs(context.Background(), &innerFields, false); err.HasError() {
			return nil, errors.New("failed to parse struct fields")
		}

		nestedFields := make([]iceberg.NestedField, len(innerFields))
		for i, innerField := range innerFields {
			typ, err := convertIcebergTableSchemaInnerField(innerField)
			if err != nil {
				return nil, err
			}
			nestedFields[i] = iceberg.NestedField{
				ID:       int(innerField.ID.ValueInt64()),
				Name:     innerField.Name.ValueString(),
				Type:     typ,
				Required: innerField.Required.ValueBool(),
				Doc:      innerField.Doc.ValueString(),
			}
		}
		return &iceberg.StructType{FieldList: nestedFields}, nil
	}
	return terraformToIcebergType(field.Type.ValueString(), field.ListProperties, field.MapProperties, types.ObjectNull(map[string]attr.Type{}))
}

func convertIcebergTableSchemaInnerField(field icebergTableSchemaInnerField) (iceberg.Type, error) {
	if field.Type.ValueString() == "struct" {
		if field.StructProperties.IsNull() || field.StructProperties.IsUnknown() {
			return nil, errors.New("struct_properties must be set for struct type")
		}
		var structProps icebergTableSchemaInnerStructProperties
		if err := field.StructProperties.As(context.Background(), &structProps, basetypes.ObjectAsOptions{}); err.HasError() {
			return nil, errors.New("failed to parse struct_properties")
		}

		var leafFields []icebergTableSchemaLeafField
		if err := structProps.Fields.ElementsAs(context.Background(), &leafFields, false); err.HasError() {
			return nil, errors.New("failed to parse struct fields")
		}

		nestedFields := make([]iceberg.NestedField, len(leafFields))
		for i, leafField := range leafFields {
			typ, err := convertIcebergTableSchemaLeafField(leafField)
			if err != nil {
				return nil, err
			}
			nestedFields[i] = iceberg.NestedField{
				ID:       int(leafField.ID.ValueInt64()),
				Name:     leafField.Name.ValueString(),
				Type:     typ,
				Required: leafField.Required.ValueBool(),
				Doc:      leafField.Doc.ValueString(),
			}
		}
		return &iceberg.StructType{FieldList: nestedFields}, nil
	}
	return terraformToIcebergType(field.Type.ValueString(), field.ListProperties, field.MapProperties, types.ObjectNull(map[string]attr.Type{}))
}

func convertIcebergTableSchemaLeafField(field icebergTableSchemaLeafField) (iceberg.Type, error) {
	if field.Type.ValueString() == "struct" {
		return nil, errors.New("maximum nesting depth reached (3 levels)")
	}
	return terraformToIcebergType(field.Type.ValueString(), field.ListProperties, field.MapProperties, types.ObjectNull(map[string]attr.Type{}))
}

var (
	decimalRegex = regexp.MustCompile(`^decimal\((\d+),\s*(\d+)\)$`)
	fixedRegex   = regexp.MustCompile(`^fixed\((\d+)\)$`)
)

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
	case "date":
		return iceberg.DateType{}, nil
	case "time":
		return iceberg.TimeType{}, nil
	case "timestamp":
		return iceberg.TimestampType{}, nil
	case "timestamptz":
		return iceberg.TimestampTzType{}, nil
	case "timestamp_ns":
		return iceberg.TimestampNsType{}, nil
	case "timestamptz_ns":
		return iceberg.TimestampTzNsType{}, nil
	case "string":
		return iceberg.StringType{}, nil
	case "uuid":
		return iceberg.UUIDType{}, nil
	case "binary":
		return iceberg.BinaryType{}, nil
	}

	if matches := decimalRegex.FindStringSubmatch(s); matches != nil {
		p, _ := strconv.Atoi(matches[1])
		s, _ := strconv.Atoi(matches[2])
		return iceberg.DecimalTypeOf(p, s), nil
	}

	if matches := fixedRegex.FindStringSubmatch(s); matches != nil {
		l, _ := strconv.Atoi(matches[1])
		return iceberg.FixedTypeOf(l), nil
	}

	return nil, errors.New("unsupported type: " + s)
}

func typeToString(t iceberg.Type) (string, error) {
	return t.String(), nil
}

func icebergToTerraformFieldCommon(field iceberg.NestedField) (string, types.Object, types.Object, error) {
	typeStr := field.Type.String()
	listProps := types.ObjectNull(icebergTableSchemaFieldListProperties{}.AttrTypes())
	mapProps := types.ObjectNull(icebergTableSchemaFieldMapProperties{}.AttrTypes())

	switch t := field.Type.(type) {
	case iceberg.DecimalType:
		// Handled by t.String() -> "decimal(P, S)"
	case iceberg.FixedType:
		// Handled by t.String() -> "fixed(L)"
	case *iceberg.ListType:
		typeStr = "list"
		elemTypeStr, err := typeToString(t.Element)
		if err != nil {
			return "", listProps, mapProps, err
		}
		listProps = types.ObjectValueMust(
			icebergTableSchemaFieldListProperties{}.AttrTypes(),
			map[string]attr.Value{
				"element_id":       types.Int64Value(int64(t.ElementID)),
				"element_type":     types.StringValue(elemTypeStr),
				"element_required": types.BoolValue(t.ElementRequired),
			},
		)
	case *iceberg.MapType:
		typeStr = "map"
		keyTypeStr, err := typeToString(t.KeyType)
		if err != nil {
			return "", listProps, mapProps, err
		}
		valueTypeStr, err := typeToString(t.ValueType)
		if err != nil {
			return "", listProps, mapProps, err
		}
		mapProps = types.ObjectValueMust(
			icebergTableSchemaFieldMapProperties{}.AttrTypes(),
			map[string]attr.Value{
				"key_id":         types.Int64Value(int64(t.KeyID)),
				"key_type":       types.StringValue(keyTypeStr),
				"value_id":       types.Int64Value(int64(t.ValueID)),
				"value_type":     types.StringValue(valueTypeStr),
				"value_required": types.BoolValue(t.ValueRequired),
			},
		)
	}
	return typeStr, listProps, mapProps, nil
}

func icebergToTerraformField(field iceberg.NestedField) (attr.Value, error) {
	typeStr, listProps, mapProps, err := icebergToTerraformFieldCommon(field)
	if err != nil {
		return nil, err
	}

	structProps := types.ObjectNull(icebergTableSchemaFieldStructProperties{}.AttrTypes())
	if t, ok := field.Type.(*iceberg.StructType); ok {
		typeStr = "struct"
		nestedFields := make([]attr.Value, len(t.Fields()))
		for i, nestedField := range t.Fields() {
			f, err := icebergToTerraformInnerField(nestedField)
			if err != nil {
				return nil, err
			}
			nestedFields[i] = f
		}
		structProps = types.ObjectValueMust(
			icebergTableSchemaFieldStructProperties{}.AttrTypes(),
			map[string]attr.Value{
				"fields": types.ListValueMust(types.ObjectType{AttrTypes: icebergTableSchemaInnerField{}.AttrTypes()}, nestedFields),
			},
		)
	}

	doc := types.StringValue(field.Doc)
	if field.Doc == "" {
		doc = types.StringNull()
	}

	return types.ObjectValueMust(
		icebergTableSchemaField{}.AttrTypes(),
		map[string]attr.Value{
			"id":                types.Int64Value(int64(field.ID)),
			"name":              types.StringValue(field.Name),
			"type":              types.StringValue(typeStr),
			"required":          types.BoolValue(field.Required),
			"doc":               doc,
			"list_properties":   listProps,
			"map_properties":    mapProps,
			"struct_properties": structProps,
		},
	), nil
}

func icebergToTerraformInnerField(field iceberg.NestedField) (attr.Value, error) {
	typeStr, listProps, mapProps, err := icebergToTerraformFieldCommon(field)
	if err != nil {
		return nil, err
	}

	structProps := types.ObjectNull(icebergTableSchemaInnerStructProperties{}.AttrTypes())
	if t, ok := field.Type.(*iceberg.StructType); ok {
		typeStr = "struct"
		nestedFields := make([]attr.Value, len(t.Fields()))
		for i, nestedField := range t.Fields() {
			f, err := icebergToTerraformLeafField(nestedField)
			if err != nil {
				return nil, err
			}
			nestedFields[i] = f
		}
		structProps = types.ObjectValueMust(
			icebergTableSchemaInnerStructProperties{}.AttrTypes(),
			map[string]attr.Value{
				"fields": types.ListValueMust(types.ObjectType{AttrTypes: icebergTableSchemaLeafField{}.AttrTypes()}, nestedFields),
			},
		)
	}

	doc := types.StringValue(field.Doc)
	if field.Doc == "" {
		doc = types.StringNull()
	}

	return types.ObjectValueMust(
		icebergTableSchemaInnerField{}.AttrTypes(),
		map[string]attr.Value{
			"id":                types.Int64Value(int64(field.ID)),
			"name":              types.StringValue(field.Name),
			"type":              types.StringValue(typeStr),
			"required":          types.BoolValue(field.Required),
			"doc":               doc,
			"list_properties":   listProps,
			"map_properties":    mapProps,
			"struct_properties": structProps,
		},
	), nil
}

func icebergToTerraformLeafField(field iceberg.NestedField) (attr.Value, error) {
	typeStr, listProps, mapProps, err := icebergToTerraformFieldCommon(field)
	if err != nil {
		return nil, err
	}

	if _, ok := field.Type.(*iceberg.StructType); ok {
		// recursion stop
		return nil, errors.New("maximum nesting depth reached (3 levels) during read")
	}

	doc := types.StringValue(field.Doc)
	if field.Doc == "" {
		doc = types.StringNull()
	}

	return types.ObjectValueMust(
		icebergTableSchemaLeafField{}.AttrTypes(),
		map[string]attr.Value{
			"id":              types.Int64Value(int64(field.ID)),
			"name":            types.StringValue(field.Name),
			"type":            types.StringValue(typeStr),
			"required":        types.BoolValue(field.Required),
			"doc":             doc,
			"list_properties": listProps,
			"map_properties":  mapProps,
		},
	), nil
}

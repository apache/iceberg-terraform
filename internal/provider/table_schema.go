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
	ID                types.Int64  `tfsdk:"id"`
	Name              types.String `tfsdk:"name"`
	Type              types.String `tfsdk:"type"`
	Required          types.Bool   `tfsdk:"required"`
	Doc               types.String `tfsdk:"doc"`
	DecimalProperties types.Object `tfsdk:"decimal_properties"`
	FixedProperties   types.Object `tfsdk:"fixed_properties"`
	ListProperties    types.Object `tfsdk:"list_properties"`
	MapProperties     types.Object `tfsdk:"map_properties"`
	StructProperties  types.Object `tfsdk:"struct_properties"`
}

func (icebergTableSchemaField) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"id":                 types.Int64Type,
		"name":               types.StringType,
		"type":               types.StringType,
		"required":           types.BoolType,
		"doc":                types.StringType,
		"decimal_properties": types.ObjectType{AttrTypes: icebergTableSchemaFieldDecimalProperties{}.AttrTypes()},
		"fixed_properties":   types.ObjectType{AttrTypes: icebergTableSchemaFieldFixedProperties{}.AttrTypes()},
		"list_properties":    types.ObjectType{AttrTypes: icebergTableSchemaFieldListProperties{}.AttrTypes()},
		"map_properties":     types.ObjectType{AttrTypes: icebergTableSchemaFieldMapProperties{}.AttrTypes()},
		"struct_properties":  types.ObjectType{AttrTypes: icebergTableSchemaFieldStructProperties{}.AttrTypes()},
	}
}

type icebergTableSchemaInnerField struct {
	ID                types.Int64  `tfsdk:"id"`
	Name              types.String `tfsdk:"name"`
	Type              types.String `tfsdk:"type"`
	Required          types.Bool   `tfsdk:"required"`
	Doc               types.String `tfsdk:"doc"`
	DecimalProperties types.Object `tfsdk:"decimal_properties"`
	FixedProperties   types.Object `tfsdk:"fixed_properties"`
	ListProperties    types.Object `tfsdk:"list_properties"`
	MapProperties     types.Object `tfsdk:"map_properties"`
}

func (icebergTableSchemaInnerField) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"id":                 types.Int64Type,
		"name":               types.StringType,
		"type":               types.StringType,
		"required":           types.BoolType,
		"doc":                types.StringType,
		"decimal_properties": types.ObjectType{AttrTypes: icebergTableSchemaFieldDecimalProperties{}.AttrTypes()},
		"fixed_properties":   types.ObjectType{AttrTypes: icebergTableSchemaFieldFixedProperties{}.AttrTypes()},
		"list_properties":    types.ObjectType{AttrTypes: icebergTableSchemaFieldListProperties{}.AttrTypes()},
		"map_properties":     types.ObjectType{AttrTypes: icebergTableSchemaFieldMapProperties{}.AttrTypes()},
	}
}

type icebergTableSchemaFieldDecimalProperties struct {
	Precision types.Int64 `tfsdk:"precision"`
	Scale     types.Int64 `tfsdk:"scale"`
}

func (icebergTableSchemaFieldDecimalProperties) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"precision": types.Int64Type,
		"scale":     types.Int64Type,
	}
}

type icebergTableSchemaFieldFixedProperties struct {
	Length types.Int64 `tfsdk:"length"`
}

func (icebergTableSchemaFieldFixedProperties) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"length": types.Int64Type,
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

func terraformToIcebergType(field icebergTableSchemaField) (iceberg.Type, error) {
	typeStr := field.Type.ValueString()
	switch typeStr {
	case "list":
		if field.ListProperties.IsNull() || field.ListProperties.IsUnknown() {
			return nil, errors.New("list_properties must be set for list type")
		}
		var listProps icebergTableSchemaFieldListProperties
		if err := field.ListProperties.As(context.Background(), &listProps, basetypes.ObjectAsOptions{}); err.HasError() {
			return nil, errors.New("failed to parse list_properties")
		}
		elemType, err := stringToType(listProps.ElementType.ValueString())
		if err != nil {
			return nil, err
		}
		return &iceberg.ListType{
			ElementID:       int(listProps.ElementID.ValueInt64()),
			Element:         elemType,
			ElementRequired: listProps.ElementRequired.ValueBool(),
		}, nil

	case "map":
		if field.MapProperties.IsNull() || field.MapProperties.IsUnknown() {
			return nil, errors.New("map_properties must be set for map type")
		}
		var mapProps icebergTableSchemaFieldMapProperties
		if err := field.MapProperties.As(context.Background(), &mapProps, basetypes.ObjectAsOptions{}); err.HasError() {
			return nil, errors.New("failed to parse map_properties")
		}
		keyType, err := stringToType(mapProps.KeyType.ValueString())
		if err != nil {
			return nil, err
		}
		valueType, err := stringToType(mapProps.ValueType.ValueString())
		if err != nil {
			return nil, err
		}
		return &iceberg.MapType{
			KeyID:         int(mapProps.KeyID.ValueInt64()),
			KeyType:       keyType,
			ValueID:       int(mapProps.ValueID.ValueInt64()),
			ValueType:     valueType,
			ValueRequired: mapProps.ValueRequired.ValueBool(),
		}, nil

	case "struct":
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
			typ, err := terraformToIcebergTypeFromInner(innerField)
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
		return &iceberg.StructType{
			Fields: nestedFields,
		}, nil

	case "decimal":
		if field.DecimalProperties.IsNull() || field.DecimalProperties.IsUnknown() {
			return nil, errors.New("decimal_properties must be set for decimal type")
		}
		var decProps icebergTableSchemaFieldDecimalProperties
		if err := field.DecimalProperties.As(context.Background(), &decProps, basetypes.ObjectAsOptions{}); err.HasError() {
			return nil, errors.New("failed to parse decimal_properties")
		}
		return iceberg.DecimalTypeOf(
			int(decProps.Precision.ValueInt64()),
			int(decProps.Scale.ValueInt64()),
		), nil

	case "fixed":
		if field.FixedProperties.IsNull() || field.FixedProperties.IsUnknown() {
			return nil, errors.New("fixed_properties must be set for fixed type")
		}
		var fixedProps icebergTableSchemaFieldFixedProperties
		if err := field.FixedProperties.As(context.Background(), &fixedProps, basetypes.ObjectAsOptions{}); err.HasError() {
			return nil, errors.New("failed to parse fixed_properties")
		}
		return iceberg.FixedTypeOf(
			int(fixedProps.Length.ValueInt64()),
		), nil
	}

	return stringToType(typeStr)
}

func terraformToIcebergTypeFromInner(field icebergTableSchemaInnerField) (iceberg.Type, error) {
	typeStr := field.Type.ValueString()
	switch typeStr {
	case "list":
		if field.ListProperties.IsNull() || field.ListProperties.IsUnknown() {
			return nil, errors.New("list_properties must be set for list type")
		}
		var listProps icebergTableSchemaFieldListProperties
		if err := field.ListProperties.As(context.Background(), &listProps, basetypes.ObjectAsOptions{}); err.HasError() {
			return nil, errors.New("failed to parse list_properties")
		}
		elemType, err := stringToType(listProps.ElementType.ValueString())
		if err != nil {
			return nil, err
		}
		return &iceberg.ListType{
			ElementID:       int(listProps.ElementID.ValueInt64()),
			Element:         elemType,
			ElementRequired: listProps.ElementRequired.ValueBool(),
		}, nil

	case "map":
		if field.MapProperties.IsNull() || field.MapProperties.IsUnknown() {
			return nil, errors.New("map_properties must be set for map type")
		}
		var mapProps icebergTableSchemaFieldMapProperties
		if err := field.MapProperties.As(context.Background(), &mapProps, basetypes.ObjectAsOptions{}); err.HasError() {
			return nil, errors.New("failed to parse map_properties")
		}
		keyType, err := stringToType(mapProps.KeyType.ValueString())
		if err != nil {
			return nil, err
		}
		valueType, err := stringToType(mapProps.ValueType.ValueString())
		if err != nil {
			return nil, err
		}
		return &iceberg.MapType{
			KeyID:         int(mapProps.KeyID.ValueInt64()),
			KeyType:       keyType,
			ValueID:       int(mapProps.ValueID.ValueInt64()),
			ValueType:     valueType,
			ValueRequired: mapProps.ValueRequired.ValueBool(),
		}, nil

	case "struct":
		return nil, errors.New("nested structs are not supported in this version")

	case "decimal":
		if field.DecimalProperties.IsNull() || field.DecimalProperties.IsUnknown() {
			return nil, errors.New("decimal_properties must be set for decimal type")
		}
		var decProps icebergTableSchemaFieldDecimalProperties
		if err := field.DecimalProperties.As(context.Background(), &decProps, basetypes.ObjectAsOptions{}); err.HasError() {
			return nil, errors.New("failed to parse decimal_properties")
		}
		return iceberg.DecimalTypeOf(
			int(decProps.Precision.ValueInt64()),
			int(decProps.Scale.ValueInt64()),
		), nil

	case "fixed":
		if field.FixedProperties.IsNull() || field.FixedProperties.IsUnknown() {
			return nil, errors.New("fixed_properties must be set for fixed type")
		}
		var fixedProps icebergTableSchemaFieldFixedProperties
		if err := field.FixedProperties.As(context.Background(), &fixedProps, basetypes.ObjectAsOptions{}); err.HasError() {
			return nil, errors.New("failed to parse fixed_properties")
		}
		return iceberg.FixedTypeOf(
			int(fixedProps.Length.ValueInt64()),
		), nil
	}

	return stringToType(typeStr)
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

func icebergToTerraformField(field iceberg.NestedField) (attr.Value, error) {
	// Defaults
	typeStr := field.Type.String()
	decimalProps := types.ObjectNull(icebergTableSchemaFieldDecimalProperties{}.AttrTypes())
	fixedProps := types.ObjectNull(icebergTableSchemaFieldFixedProperties{}.AttrTypes())
	listProps := types.ObjectNull(icebergTableSchemaFieldListProperties{}.AttrTypes())
	mapProps := types.ObjectNull(icebergTableSchemaFieldMapProperties{}.AttrTypes())
	structProps := types.ObjectNull(icebergTableSchemaFieldStructProperties{}.AttrTypes())

	switch t := field.Type.(type) {
	case iceberg.DecimalType:
		typeStr = "decimal"
		decimalProps = types.ObjectValueMust(
			icebergTableSchemaFieldDecimalProperties{}.AttrTypes(),
			map[string]attr.Value{
				"precision": types.Int64Value(int64(t.Precision())),
				"scale":     types.Int64Value(int64(t.Scale())),
			},
		)
	case iceberg.FixedType:
		typeStr = "fixed"
		fixedProps = types.ObjectValueMust(
			icebergTableSchemaFieldFixedProperties{}.AttrTypes(),
			map[string]attr.Value{
				"length": types.Int64Value(int64(t.Len())),
			},
		)
	case *iceberg.ListType:
		typeStr = "list"
		elemTypeStr, err := typeToString(t.Element)
		if err != nil {
			return nil, err
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
			return nil, err
		}
		valueTypeStr, err := typeToString(t.ValueType)
		if err != nil {
			return nil, err
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
	case *iceberg.StructType:
		typeStr = "struct"
		nestedFields := make([]attr.Value, len(t.Fields))
		for i, nestedField := range t.Fields {
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
			"id":                 types.Int64Value(int64(field.ID)),
			"name":               types.StringValue(field.Name),
			"type":               types.StringValue(typeStr),
			"required":           types.BoolValue(field.Required),
			"doc":                doc,
			"decimal_properties": decimalProps,
			"fixed_properties":   fixedProps,
			"list_properties":    listProps,
			"map_properties":     mapProps,
			"struct_properties":  structProps,
		},
	), nil
}

func icebergToTerraformInnerField(field iceberg.NestedField) (attr.Value, error) {
	typeStr := field.Type.String()
	decimalProps := types.ObjectNull(icebergTableSchemaFieldDecimalProperties{}.AttrTypes())
	fixedProps := types.ObjectNull(icebergTableSchemaFieldFixedProperties{}.AttrTypes())
	listProps := types.ObjectNull(icebergTableSchemaFieldListProperties{}.AttrTypes())
	mapProps := types.ObjectNull(icebergTableSchemaFieldMapProperties{}.AttrTypes())

	switch t := field.Type.(type) {
	case iceberg.DecimalType:
		typeStr = "decimal"
		decimalProps = types.ObjectValueMust(
			icebergTableSchemaFieldDecimalProperties{}.AttrTypes(),
			map[string]attr.Value{
				"precision": types.Int64Value(int64(t.Precision())),
				"scale":     types.Int64Value(int64(t.Scale())),
			},
		)
	case iceberg.FixedType:
		typeStr = "fixed"
		fixedProps = types.ObjectValueMust(
			icebergTableSchemaFieldFixedProperties{}.AttrTypes(),
			map[string]attr.Value{
				"length": types.Int64Value(int64(t.Len())),
			},
		)
	case *iceberg.ListType:
		typeStr = "list"
		elemTypeStr, err := typeToString(t.Element)
		if err != nil {
			return nil, err
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
			return nil, err
		}
		valueTypeStr, err := typeToString(t.ValueType)
		if err != nil {
			return nil, err
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
	case *iceberg.StructType:
		return nil, errors.New("nested structs are not supported in this version")
	}

	doc := types.StringValue(field.Doc)
	if field.Doc == "" {
		doc = types.StringNull()
	}

	return types.ObjectValueMust(
		icebergTableSchemaInnerField{}.AttrTypes(),
		map[string]attr.Value{
			"id":                 types.Int64Value(int64(field.ID)),
			"name":               types.StringValue(field.Name),
			"type":               types.StringValue(typeStr),
			"required":           types.BoolValue(field.Required),
			"doc":                doc,
			"decimal_properties": decimalProps,
			"fixed_properties":   fixedProps,
			"list_properties":    listProps,
			"map_properties":     mapProps,
		},
	), nil
}

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
	ID       types.Int64                 `tfsdk:"id"`
	Name     types.String                `tfsdk:"name"`
	Type     icebergTableSchemaFieldType `tfsdk:"type"`
	Required types.Bool                  `tfsdk:"required"`
	Doc      types.String                `tfsdk:"doc"`
}

func (icebergTableSchemaField) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"id":       types.Int64Type,
		"name":     types.StringType,
		"type":     types.ObjectType{AttrTypes: icebergTableSchemaFieldType{}.AttrTypes()},
		"required": types.BoolType,
		"doc":      types.StringType,
	}
}

type icebergTableSchemaFieldType struct {
	Primitive types.String `tfsdk:"primitive"`
	List      types.Object `tfsdk:"list"`
	Map       types.Object `tfsdk:"map"`
}

func (icebergTableSchemaFieldType) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"primitive": types.StringType,
		"list":      types.ObjectType{AttrTypes: icebergTableSchemaFieldTypeList{}.AttrTypes()},
		"map":       types.ObjectType{AttrTypes: icebergTableSchemaFieldTypeMap{}.AttrTypes()},
	}
}

type icebergTableSchemaFieldTypeList struct {
	ElementID       types.Int64  `tfsdk:"element_id"`
	ElementType     types.String `tfsdk:"element_type"`
	ElementRequired types.Bool   `tfsdk:"element_required"`
}

func (icebergTableSchemaFieldTypeList) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"element_id":       types.Int64Type,
		"element_type":     types.StringType,
		"element_required": types.BoolType,
	}
}

type icebergTableSchemaFieldTypeMap struct {
	KeyID         types.Int64  `tfsdk:"key_id"`
	KeyType       types.String `tfsdk:"key_type"`
	ValueID       types.Int64  `tfsdk:"value_id"`
	ValueType     types.String `tfsdk:"value_type"`
	ValueRequired types.Bool   `tfsdk:"value_required"`
}

func (icebergTableSchemaFieldTypeMap) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"key_id":         types.Int64Type,
		"key_type":       types.StringType,
		"value_id":       types.Int64Type,
		"value_type":     types.StringType,
		"value_required": types.BoolType,
	}
}

func terraformTypeToIcebergType(typ icebergTableSchemaFieldType) (iceberg.Type, error) {
	if !typ.Primitive.IsNull() {
		return stringToType(typ.Primitive.ValueString())
	}

	if !typ.List.IsNull() {
		var list icebergTableSchemaFieldTypeList
		if err := typ.List.As(context.Background(), &list, basetypes.ObjectAsOptions{}); err.HasError() {
			return nil, errors.New("failed to parse list type")
		}

		elemType, err := stringToType(list.ElementType.ValueString())
		if err != nil {
			return nil, err
		}

		return &iceberg.ListType{
			ElementID:       int(list.ElementID.ValueInt64()),
			Element:         elemType,
			ElementRequired: list.ElementRequired.ValueBool(),
		}, nil
	}

	if !typ.Map.IsNull() {
		var m icebergTableSchemaFieldTypeMap
		if err := typ.Map.As(context.Background(), &m, basetypes.ObjectAsOptions{}); err.HasError() {
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

		return &iceberg.MapType{
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

func icebergTypeToTerraformType(t iceberg.Type) (attr.Value, error) {
	switch typ := t.(type) {
	case iceberg.BooleanType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("boolean"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case iceberg.Int32Type:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("int"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case iceberg.Int64Type:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("long"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case iceberg.Float32Type:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("float"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case iceberg.Float64Type:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("double"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case iceberg.DecimalType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("decimal"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case iceberg.DateType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("date"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case iceberg.TimeType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("time"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case iceberg.TimestampType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("timestamp"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case iceberg.TimestampTzType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("timestamptz"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case iceberg.StringType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("string"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case iceberg.UUIDType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("uuid"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case iceberg.FixedType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("fixed"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case iceberg.BinaryType:
		return types.ObjectValueMust(
			icebergTableSchemaFieldType{}.AttrTypes(),
			map[string]attr.Value{
				"primitive": types.StringValue("binary"),
				"list":      types.ObjectNull(icebergTableSchemaFieldTypeList{}.AttrTypes()),
				"map":       types.ObjectNull(icebergTableSchemaFieldTypeMap{}.AttrTypes()),
			},
		), nil
	case *iceberg.ListType:
		elementType, err := icebergTypeToTerraformType(typ.Element)
		if err != nil {
			return types.ObjectNull(icebergTableSchemaFieldType{}.AttrTypes()), err
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
		), nil
	case *iceberg.MapType:
		keyType, err := icebergTypeToTerraformType(typ.KeyType)
		if err != nil {
			return types.ObjectNull(icebergTableSchemaFieldType{}.AttrTypes()), err
		}
		valueType, err := icebergTypeToTerraformType(typ.ValueType)
		if err != nil {
			return types.ObjectNull(icebergTableSchemaFieldType{}.AttrTypes()), err
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
		), nil
	}
	return types.ObjectNull(icebergTableSchemaFieldType{}.AttrTypes()), errors.New("unsupported type: " + t.String())
}

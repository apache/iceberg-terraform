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
	"encoding/json"

	"github.com/apache/iceberg-go"
	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type icebergTableSchema struct {
	ID     types.Int64               `tfsdk:"id" json:"schema-id"`
	Fields []icebergTableSchemaField `tfsdk:"fields" json:"fields"`
}

func (s icebergTableSchema) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"id": types.Int64Type,
		"fields": types.ListType{
			ElemType: types.ObjectType{
				AttrTypes: icebergTableSchemaField{}.AttrTypes(),
			},
		},
	}
}

func (s icebergTableSchema) MarshalJSON() ([]byte, error) {
	type Alias struct {
		ID     int64                     `json:"schema-id"`
		Fields []icebergTableSchemaField `json:"fields"`
	}
	var id int64
	if !s.ID.IsNull() && !s.ID.IsUnknown() {
		id = s.ID.ValueInt64()
	}
	return json.Marshal(&struct {
		Type string `json:"type"`
		Alias
	}{
		Type: "struct",
		Alias: Alias{
			ID:     id,
			Fields: s.Fields,
		},
	})
}

func (s *icebergTableSchema) UnmarshalJSON(b []byte) error {
	var raw struct {
		ID     int64                     `json:"schema-id"`
		Fields []icebergTableSchemaField `json:"fields"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	s.ID = types.Int64Value(raw.ID)
	s.Fields = raw.Fields
	return nil
}

func (s *icebergTableSchema) ToIceberg() (*iceberg.Schema, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	var icebergSchema iceberg.Schema
	if err := json.Unmarshal(b, &icebergSchema); err != nil {
		return nil, err
	}
	return &icebergSchema, nil
}

func (s *icebergTableSchema) FromIceberg(icebergSchema *iceberg.Schema) error {
	b, err := json.Marshal(icebergSchema)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, s)
}

type icebergTableSchemaField struct {
	ID               types.Int64                              `tfsdk:"id" json:"id"`
	Name             string                                   `tfsdk:"name" json:"name"`
	Type             string                                   `tfsdk:"type" json:"-"`
	Required         bool                                     `tfsdk:"required" json:"required"`
	Doc              *string                                  `tfsdk:"doc" json:"doc,omitempty"`
	ListProperties   *icebergTableSchemaFieldListProperties   `tfsdk:"list_properties" json:"-"`
	MapProperties    *icebergTableSchemaFieldMapProperties    `tfsdk:"map_properties" json:"-"`
	StructProperties *icebergTableSchemaFieldStructProperties `tfsdk:"struct_properties" json:"-"`
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

func (f icebergTableSchemaField) MarshalJSON() ([]byte, error) {
	return marshalFieldJSON(f.ID, f.Name, f.Type, f.Required, f.Doc, f.ListProperties, f.MapProperties, f.StructProperties)
}

func (f *icebergTableSchemaField) UnmarshalJSON(b []byte) error {
	return unmarshalFieldJSON(b, &f.ID, &f.Name, &f.Type, &f.Required, &f.Doc, &f.ListProperties, &f.MapProperties, &f.StructProperties)
}

type icebergTableSchemaInnerField struct {
	ID               types.Int64                              `tfsdk:"id" json:"id"`
	Name             string                                   `tfsdk:"name" json:"name"`
	Type             string                                   `tfsdk:"type" json:"-"`
	Required         bool                                     `tfsdk:"required" json:"required"`
	Doc              *string                                  `tfsdk:"doc" json:"doc,omitempty"`
	ListProperties   *icebergTableSchemaFieldListProperties   `tfsdk:"list_properties" json:"-"`
	MapProperties    *icebergTableSchemaFieldMapProperties    `tfsdk:"map_properties" json:"-"`
	StructProperties *icebergTableSchemaInnerStructProperties `tfsdk:"struct_properties" json:"-"`
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

func (f icebergTableSchemaInnerField) MarshalJSON() ([]byte, error) {
	return marshalFieldJSON(f.ID, f.Name, f.Type, f.Required, f.Doc, f.ListProperties, f.MapProperties, f.StructProperties)
}

func (f *icebergTableSchemaInnerField) UnmarshalJSON(b []byte) error {
	return unmarshalFieldJSON(b, &f.ID, &f.Name, &f.Type, &f.Required, &f.Doc, &f.ListProperties, &f.MapProperties, &f.StructProperties)
}

type icebergTableSchemaLeafField struct {
	ID             types.Int64                            `tfsdk:"id" json:"id"`
	Name           string                                 `tfsdk:"name" json:"name"`
	Type           string                                 `tfsdk:"type" json:"-"`
	Required       bool                                   `tfsdk:"required" json:"required"`
	Doc            *string                                `tfsdk:"doc" json:"doc,omitempty"`
	ListProperties *icebergTableSchemaFieldListProperties `tfsdk:"list_properties" json:"-"`
	MapProperties  *icebergTableSchemaFieldMapProperties  `tfsdk:"map_properties" json:"-"`
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

func (f icebergTableSchemaLeafField) MarshalJSON() ([]byte, error) {
	return marshalFieldJSON(f.ID, f.Name, f.Type, f.Required, f.Doc, f.ListProperties, f.MapProperties, nil)
}

func (f *icebergTableSchemaLeafField) UnmarshalJSON(b []byte) error {
	var structProps *json.RawMessage // unused for leaf
	return unmarshalFieldJSON(b, &f.ID, &f.Name, &f.Type, &f.Required, &f.Doc, &f.ListProperties, &f.MapProperties, &structProps)
}

type icebergTableSchemaFieldListProperties struct {
	ElementID       types.Int64 `tfsdk:"element_id" json:"element-id"`
	ElementType     string      `tfsdk:"element_type" json:"element"`
	ElementRequired bool        `tfsdk:"element_required" json:"element-required"`
}

func (icebergTableSchemaFieldListProperties) AttrTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"element_id":       types.Int64Type,
		"element_type":     types.StringType,
		"element_required": types.BoolType,
	}
}

func (p icebergTableSchemaFieldListProperties) MarshalJSON() ([]byte, error) {
	var elementID int64
	if !p.ElementID.IsNull() && !p.ElementID.IsUnknown() {
		elementID = p.ElementID.ValueInt64()
	}
	return json.Marshal(struct {
		Type            string `json:"type"`
		ElementID       int64  `json:"element-id"`
		ElementType     string `json:"element"`
		ElementRequired bool   `json:"element-required"`
	}{
		Type:            "list",
		ElementID:       elementID,
		ElementType:     p.ElementType,
		ElementRequired: p.ElementRequired,
	})
}

func (p *icebergTableSchemaFieldListProperties) UnmarshalJSON(b []byte) error {
	var raw struct {
		ElementID       int64  `json:"element-id"`
		ElementType     string `json:"element"`
		ElementRequired bool   `json:"element-required"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	p.ElementID = types.Int64Value(raw.ElementID)
	p.ElementType = raw.ElementType
	p.ElementRequired = raw.ElementRequired
	return nil
}

type icebergTableSchemaFieldMapProperties struct {
	KeyID         types.Int64 `tfsdk:"key_id" json:"key-id"`
	KeyType       string      `tfsdk:"key_type" json:"key"`
	ValueID       types.Int64 `tfsdk:"value_id" json:"value-id"`
	ValueType     string      `tfsdk:"value_type" json:"value"`
	ValueRequired bool        `tfsdk:"value_required" json:"value-required"`
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

func (p icebergTableSchemaFieldMapProperties) MarshalJSON() ([]byte, error) {
	var keyID, valueID int64
	if !p.KeyID.IsNull() && !p.KeyID.IsUnknown() {
		keyID = p.KeyID.ValueInt64()
	}
	if !p.ValueID.IsNull() && !p.ValueID.IsUnknown() {
		valueID = p.ValueID.ValueInt64()
	}
	return json.Marshal(struct {
		Type          string `json:"type"`
		KeyID         int64  `json:"key-id"`
		KeyType       string `json:"key"`
		ValueID       int64  `json:"value-id"`
		ValueType     string `json:"value"`
		ValueRequired bool   `json:"value-required"`
	}{
		Type:          "map",
		KeyID:         keyID,
		KeyType:       p.KeyType,
		ValueID:       valueID,
		ValueType:     p.ValueType,
		ValueRequired: p.ValueRequired,
	})
}

func (p *icebergTableSchemaFieldMapProperties) UnmarshalJSON(b []byte) error {
	var raw struct {
		KeyID         int64  `json:"key-id"`
		KeyType       string `json:"key"`
		ValueID       int64  `json:"value-id"`
		ValueType     string `json:"value"`
		ValueRequired bool   `json:"value-required"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	p.KeyID = types.Int64Value(raw.KeyID)
	p.KeyType = raw.KeyType
	p.ValueID = types.Int64Value(raw.ValueID)
	p.ValueType = raw.ValueType
	p.ValueRequired = raw.ValueRequired
	return nil
}

type icebergTableSchemaFieldStructProperties struct {
	Fields []icebergTableSchemaInnerField `tfsdk:"fields" json:"fields"`
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

func (s icebergTableSchemaFieldStructProperties) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type   string                         `json:"type"`
		Fields []icebergTableSchemaInnerField `json:"fields"`
	}{
		Type:   "struct",
		Fields: s.Fields,
	})
}

func (s *icebergTableSchemaFieldStructProperties) UnmarshalJSON(b []byte) error {
	var raw struct {
		Fields []icebergTableSchemaInnerField `json:"fields"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	s.Fields = raw.Fields
	return nil
}

type icebergTableSchemaInnerStructProperties struct {
	Fields []icebergTableSchemaLeafField `tfsdk:"fields" json:"fields"`
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

func (s icebergTableSchemaInnerStructProperties) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type   string                        `json:"type"`
		Fields []icebergTableSchemaLeafField `json:"fields"`
	}{
		Type:   "struct",
		Fields: s.Fields,
	})
}

func (s *icebergTableSchemaInnerStructProperties) UnmarshalJSON(b []byte) error {
	var raw struct {
		Fields []icebergTableSchemaLeafField `json:"fields"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	s.Fields = raw.Fields
	return nil
}

// Helpers for shared logic

func marshalFieldJSON(id types.Int64, name, typeStr string, required bool, doc *string, listProps, mapProps, structProps interface{}) ([]byte, error) {
	type Field struct {
		ID       int64       `json:"id"`
		Name     string      `json:"name"`
		Type     interface{} `json:"type"`
		Required bool        `json:"required"`
		Doc      *string     `json:"doc,omitempty"`
	}

	var idVal int64
	if !id.IsNull() && !id.IsUnknown() {
		idVal = id.ValueInt64()
	}

	f := Field{
		ID:       idVal,
		Name:     name,
		Required: required,
		Doc:      doc,
	}

	switch typeStr {
	case "list":
		f.Type = listProps
	case "map":
		f.Type = mapProps
	case "struct":
		f.Type = structProps
	default:
		f.Type = typeStr
	}

	return json.Marshal(f)
}

func unmarshalFieldJSON(b []byte, id *types.Int64, name, typeStr *string, required *bool, doc **string, listProps, mapProps, structProps interface{}) error {
	var raw struct {
		ID       int64           `json:"id"`
		Name     string          `json:"name"`
		Type     json.RawMessage `json:"type"`
		Required bool            `json:"required"`
		Doc      *string         `json:"doc"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	*id = types.Int64Value(raw.ID)
	*name = raw.Name
	*required = raw.Required
	*doc = raw.Doc

	if len(raw.Type) > 0 && raw.Type[0] == '"' {
		var s string
		if err := json.Unmarshal(raw.Type, &s); err != nil {
			return err
		}
		*typeStr = s
	} else {
		var typeObj struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw.Type, &typeObj); err != nil {
			return err
		}
		*typeStr = typeObj.Type
		switch typeObj.Type {
		case "list":
			return json.Unmarshal(raw.Type, listProps)
		case "map":
			return json.Unmarshal(raw.Type, mapProps)
		case "struct":
			return json.Unmarshal(raw.Type, structProps)
		}
	}
	return nil
}


package dbrecord

import "slices"

const (
	FieldTypeInt    = 0
	FieldTypeString = 1
)

type FieldInfo struct {
	fieldType int
	// 文字長など。メモリ上でのサイズではない
	length int
}

type Schema struct {
	fields []string
	info   map[string]FieldInfo
}

func NewSchema() *Schema {
	return &Schema{
		fields: []string{},
		info:   make(map[string]FieldInfo),
	}
}

func (s *Schema) AddField(fieldName string, fieldType, length int) {
	s.fields = append(s.fields, fieldName)
	s.info[fieldName] = FieldInfo{fieldType: fieldType, length: length}
}

func (s *Schema) AddIntField(fieldName string) {
	s.AddField(fieldName, FieldTypeInt, 0)
}

func (s *Schema) AddStringField(fieldName string, length int) {
	s.AddField(fieldName, FieldTypeString, length)
}

func (s *Schema) Add(fieldName string, schema *Schema) {
	s.AddField(fieldName, schema.FieldType(fieldName), schema.Length(fieldName))
}

func (s *Schema) AddAll(schema *Schema) {
	for _, fieldName := range schema.fields {
		s.Add(fieldName, schema)
	}
}

func (s *Schema) FieldType(fieldName string) int {
	return s.info[fieldName].fieldType
}

func (s *Schema) Length(fieldName string) int {
	return s.info[fieldName].length
}

func (s *Schema) Fields() []string {
	return s.fields
}

func (s *Schema) HasField(fieldName string) bool {
	return slices.Contains(s.fields, fieldName)
}

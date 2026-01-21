package dbrecord

import (
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dbsize"
)

// schemaのフィールドの配置情報
type Layout struct {
	schema   *Schema
	offsets  map[string]int
	slotSize int
}

func NewLayout(schema *Schema) *Layout {
	layout := Layout{schema: schema}
	pos := dbsize.IntSize
	offsets := make(map[string]int)
	for _, field := range schema.fields {
		offsets[field] = pos
		pos += layout.LengthInBytes(field)
	}
	layout.offsets = offsets
	layout.slotSize = pos
	return &layout
}

func NewLayoutFromOffsets(schema *Schema, offsets map[string]int, slotSize int) *Layout {
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
}

func (l *Layout) Schema() *Schema {
	return l.schema
}

func (l *Layout) Offset(fieldName string) int {
	return l.offsets[fieldName]
}

func (l *Layout) SlotSize() int {
	return l.slotSize
}

// layout上でのfield valueのサイズ
func (l *Layout) LengthInBytes(fieldName string) int {
	switch l.schema.FieldType(fieldName) {
	case FieldTypeInt:
		return dbsize.IntSize
	case FieldTypeString:
		return dbfile.MaxStringLengthOnPage(l.schema.Length(fieldName))
	}
	return 0
}

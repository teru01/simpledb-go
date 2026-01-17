package dbrecord_test

import (
	"testing"

	"github.com/teru01/simpledb-go/dbrecord"
)

func TestAddIntField(t *testing.T) {
	schema := &dbrecord.Schema{}
	schema.AddIntField("id")

	if !schema.HasField("id") {
		t.Error("Schema should have field 'id'")
	}
	if schema.FieldType("id") != dbrecord.FieldTypeInt {
		t.Errorf("Field 'id' should be FieldTypeInt, got %d", schema.FieldType("id"))
	}
	if schema.Length("id") != 0 {
		t.Errorf("Int field 'id' should have length 0, got %d", schema.Length("id"))
	}
}

func TestAddStringField(t *testing.T) {
	schema := &dbrecord.Schema{}
	schema.AddStringField("name", 20)

	if !schema.HasField("name") {
		t.Error("Schema should have field 'name'")
	}
	if schema.FieldType("name") != dbrecord.FieldTypeString {
		t.Errorf("Field 'name' should be FieldTypeString, got %d", schema.FieldType("name"))
	}
	if schema.Length("name") != 20 {
		t.Errorf("String field 'name' should have length 20, got %d", schema.Length("name"))
	}
}

func TestAddField(t *testing.T) {
	schema := &dbrecord.Schema{}
	schema.AddField("age", dbrecord.FieldTypeInt, 0)
	schema.AddField("address", dbrecord.FieldTypeString, 50)

	if !schema.HasField("age") {
		t.Error("Schema should have field 'age'")
	}
	if !schema.HasField("address") {
		t.Error("Schema should have field 'address'")
	}
	if schema.FieldType("age") != dbrecord.FieldTypeInt {
		t.Errorf("Field 'age' should be FieldTypeInt, got %d", schema.FieldType("age"))
	}
	if schema.FieldType("address") != dbrecord.FieldTypeString {
		t.Errorf("Field 'address' should be FieldTypeString, got %d", schema.FieldType("address"))
	}
	if schema.Length("address") != 50 {
		t.Errorf("Field 'address' should have length 50, got %d", schema.Length("address"))
	}
}

func TestFields(t *testing.T) {
	schema := &dbrecord.Schema{}
	schema.AddIntField("id")
	schema.AddStringField("name", 30)
	schema.AddIntField("age")

	fields := schema.Fields()
	if len(fields) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(fields))
	}

	expected := []string{"id", "name", "age"}
	for i, field := range fields {
		if field != expected[i] {
			t.Errorf("Field at index %d should be %s, got %s", i, expected[i], field)
		}
	}
}

func TestHasField(t *testing.T) {
	schema := &dbrecord.Schema{}
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	if !schema.HasField("id") {
		t.Error("Schema should have field 'id'")
	}
	if !schema.HasField("name") {
		t.Error("Schema should have field 'name'")
	}
	if schema.HasField("nonexistent") {
		t.Error("Schema should not have field 'nonexistent'")
	}
}

func TestAdd(t *testing.T) {
	schema1 := &dbrecord.Schema{}
	schema1.AddIntField("id")
	schema1.AddStringField("name", 20)

	schema2 := &dbrecord.Schema{}
	schema2.Add("id", *schema1)

	if !schema2.HasField("id") {
		t.Error("Schema2 should have field 'id'")
	}
	if schema2.FieldType("id") != dbrecord.FieldTypeInt {
		t.Errorf("Field 'id' should be FieldTypeInt, got %d", schema2.FieldType("id"))
	}
}

func TestAddAll(t *testing.T) {
	schema1 := &dbrecord.Schema{}
	schema1.AddIntField("id")
	schema1.AddStringField("name", 20)
	schema1.AddIntField("age")

	schema2 := &dbrecord.Schema{}
	schema2.AddAll(*schema1)

	if !schema2.HasField("id") {
		t.Error("Schema2 should have field 'id'")
	}
	if !schema2.HasField("name") {
		t.Error("Schema2 should have field 'name'")
	}
	if !schema2.HasField("age") {
		t.Error("Schema2 should have field 'age'")
	}

	if schema2.FieldType("id") != dbrecord.FieldTypeInt {
		t.Errorf("Field 'id' should be FieldTypeInt, got %d", schema2.FieldType("id"))
	}
	if schema2.FieldType("name") != dbrecord.FieldTypeString {
		t.Errorf("Field 'name' should be FieldTypeString, got %d", schema2.FieldType("name"))
	}
	if schema2.Length("name") != 20 {
		t.Errorf("Field 'name' should have length 20, got %d", schema2.Length("name"))
	}
}

func TestMultipleFields(t *testing.T) {
	schema := &dbrecord.Schema{}
	schema.AddIntField("id")
	schema.AddStringField("name", 30)
	schema.AddIntField("age")
	schema.AddStringField("email", 50)

	testCases := []struct {
		fieldName    string
		expectedType int
		expectedLen  int
		shouldExist  bool
	}{
		{"id", dbrecord.FieldTypeInt, 0, true},
		{"name", dbrecord.FieldTypeString, 30, true},
		{"age", dbrecord.FieldTypeInt, 0, true},
		{"email", dbrecord.FieldTypeString, 50, true},
		{"nonexistent", 0, 0, false},
	}

	for _, tc := range testCases {
		if schema.HasField(tc.fieldName) != tc.shouldExist {
			t.Errorf("Field '%s' existence mismatch: expected %v", tc.fieldName, tc.shouldExist)
		}
		if tc.shouldExist {
			if schema.FieldType(tc.fieldName) != tc.expectedType {
				t.Errorf("Field '%s' type mismatch: expected %d, got %d", tc.fieldName, tc.expectedType, schema.FieldType(tc.fieldName))
			}
			if schema.Length(tc.fieldName) != tc.expectedLen {
				t.Errorf("Field '%s' length mismatch: expected %d, got %d", tc.fieldName, tc.expectedLen, schema.Length(tc.fieldName))
			}
		}
	}
}

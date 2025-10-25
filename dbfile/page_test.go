package dbfile_test

import (
	"testing"

	"github.com/teru01/simpledb-go/dbfile"
)

func TestPageInt(t *testing.T) {
	p := dbfile.NewPage(400)

	testCases := []struct {
		offset int
		value  int
	}{
		{0, 42},
		{8, -100},
		{16, 2147483647},
		{24, -2147483648},
		{100, 0},
	}

	for _, tc := range testCases {
		p.SetInt(tc.offset, tc.value)
		got := p.GetInt(tc.offset)
		if got != tc.value {
			t.Errorf("SetInt/GetInt at offset %d: expected %d, got %d", tc.offset, tc.value, got)
		}
	}
}

func TestPageString(t *testing.T) {
	p := dbfile.NewPage(400)

	testCases := []struct {
		offset int
		value  string
	}{
		{0, "hello"},
		{50, "world"},
		{100, ""},
		{150, "日本語テスト"},
		{200, "a very long string with many characters"},
	}

	for _, tc := range testCases {
		p.SetString(tc.offset, tc.value)
		got := p.GetString(tc.offset)
		if got != tc.value {
			t.Errorf("SetString/GetString at offset %d: expected %q, got %q", tc.offset, tc.value, got)
		}
	}
}

func TestPageBytes(t *testing.T) {
	p := dbfile.NewPage(400)

	testCases := []struct {
		offset int
		value  []byte
	}{
		{0, []byte{1, 2, 3, 4, 5}},
		{50, []byte{}},
		{100, []byte{0xff, 0x00, 0xaa, 0x55}},
	}

	for _, tc := range testCases {
		p.SetBytes(tc.offset, tc.value)
		got := p.GetBytes(tc.offset)
		if len(got) != len(tc.value) {
			t.Errorf("SetBytes/GetBytes at offset %d: length mismatch, expected %d, got %d", tc.offset, len(tc.value), len(got))
			continue
		}
		for i := range tc.value {
			if got[i] != tc.value[i] {
				t.Errorf("SetBytes/GetBytes at offset %d: byte %d mismatch, expected %d, got %d", tc.offset, i, tc.value[i], got[i])
			}
		}
	}
}

func TestPageMaxLength(t *testing.T) {
	p := dbfile.NewPage(400)

	testCases := []struct {
		strLen   int
		expected int
	}{
		{0, 8},     // intSize(8) + 0*4
		{1, 12},    // intSize(8) + 1*4
		{10, 48},   // intSize(8) + 10*4
		{100, 408}, // intSize(8) + 100*4
	}

	for _, tc := range testCases {
		got := p.MaxLength(tc.strLen)
		if got != tc.expected {
			t.Errorf("MaxLength(%d): expected %d, got %d", tc.strLen, tc.expected, got)
		}
	}
}

func TestNewPageFromBytes(t *testing.T) {
	original := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	p := dbfile.NewPageFromBytes(original)

	// Verify the page was initialized with the provided bytes
	// We can check by reading back an int (first 8 bytes on 64-bit systems)
	val := p.GetInt(0)
	if val == 0 && original[0] != 0 {
		t.Error("NewPageFromBytes: page does not contain expected data")
	}
}

func TestPageMixedOperations(t *testing.T) {
	p := dbfile.NewPage(400)

	// Write int at offset 0
	p.SetInt(0, 12345)

	// Write string at offset 20
	p.SetString(20, "test")

	// Write bytes at offset 100
	p.SetBytes(100, []byte{0xde, 0xad, 0xbe, 0xef})

	// Verify all values are preserved
	if got := p.GetInt(0); got != 12345 {
		t.Errorf("GetInt: expected 12345, got %d", got)
	}
	if got := p.GetString(20); got != "test" {
		t.Errorf("GetString: expected 'test', got %q", got)
	}
	bytes := p.GetBytes(100)
	expected := []byte{0xde, 0xad, 0xbe, 0xef}
	if len(bytes) != len(expected) {
		t.Errorf("GetBytes: length mismatch, expected %d, got %d", len(expected), len(bytes))
	} else {
		for i := range expected {
			if bytes[i] != expected[i] {
				t.Errorf("GetBytes: byte %d mismatch, expected 0x%x, got 0x%x", i, expected[i], bytes[i])
			}
		}
	}
}

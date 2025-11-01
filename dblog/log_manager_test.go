package dblog_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
)

func setupTestDir(t *testing.T) (*os.File, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "logmanager_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		t.Fatalf("failed to open temp dir: %v", err)
	}
	cleanup := func() {
		dirFile.Close()
		os.RemoveAll(dir)
	}
	return dirFile, cleanup
}

func TestLogManagerAppend(t *testing.T) {
	dir, cleanup := setupTestDir(t)
	defer cleanup()

	fm, err := dbfile.NewFileManager(dir, 400)
	if err != nil {
		t.Fatalf("failed to create file manager: %v", err)
	}

	lm, err := dblog.NewLogManager(fm, "test.log")
	if err != nil {
		t.Fatalf("failed to create log manager: %v", err)
	}

	testCases := []struct {
		name   string
		record []byte
	}{
		{"simple record", []byte("log record 1")},
		{"empty record", []byte{}},
		{"binary record", []byte{0x01, 0x02, 0x03, 0xff, 0xfe}},
		{"large record", []byte("this is a longer log record with more data")},
	}

	lsns := make([]int, 0, len(testCases))

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lsn, err := lm.Append(tc.record)
			if err != nil {
				t.Fatalf("Append failed: %v", err)
			}
			if lsn <= 0 {
				t.Errorf("expected positive LSN, got %d", lsn)
			}
			lsns = append(lsns, lsn)
		})
	}

	// Verify LSNs are increasing
	for i := 1; i < len(lsns); i++ {
		if lsns[i] <= lsns[i-1] {
			t.Errorf("LSNs not increasing: lsns[%d]=%d, lsns[%d]=%d", i-1, lsns[i-1], i, lsns[i])
		}
	}
}

func TestLogManagerIterator(t *testing.T) {
	dir, cleanup := setupTestDir(t)
	defer cleanup()

	fm, err := dbfile.NewFileManager(dir, 400)
	if err != nil {
		t.Fatalf("failed to create file manager: %v", err)
	}

	lm, err := dblog.NewLogManager(fm, "test.log")
	if err != nil {
		t.Fatalf("failed to create log manager: %v", err)
	}

	// Append some records
	records := [][]byte{
		[]byte("record 1"),
		[]byte("record 2"),
		[]byte("record 3"),
		[]byte("record 4"),
	}

	for _, rec := range records {
		if _, err := lm.Append(rec); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Get iterator and verify records
	iter, err := lm.Iterator()
	if err != nil {
		t.Fatalf("failed to create iterator: %v", err)
	}

	var retrieved [][]byte
	for rec, err := range iter {
		if err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		retrieved = append(retrieved, rec)
	}

	// Iterator returns records in reverse order
	if len(retrieved) != len(records) {
		t.Errorf("expected %d records, got %d", len(records), len(retrieved))
	}

	for i, rec := range retrieved {
		expected := records[len(records)-1-i]
		if len(rec) != len(expected) {
			t.Errorf("record %d: length mismatch, expected %d, got %d", i, len(expected), len(rec))
			continue
		}
		for j := range expected {
			if rec[j] != expected[j] {
				t.Errorf("record %d: byte %d mismatch, expected %d, got %d", i, j, expected[j], rec[j])
			}
		}
	}
}

func TestLogManagerMultipleBlocks(t *testing.T) {
	dirFile, cleanup := setupTestDir(t)
	defer cleanup()

	blockSize := 100
	fm, err := dbfile.NewFileManager(dirFile, blockSize)
	if err != nil {
		t.Fatalf("failed to create file manager: %v", err)
	}

	lm, err := dblog.NewLogManager(fm, "test.log")
	if err != nil {
		t.Fatalf("failed to create log manager: %v", err)
	}

	// Append enough records to span multiple blocks
	largeRecord := make([]byte, 50)
	for i := range largeRecord {
		largeRecord[i] = byte(i % 256)
	}

	appendCount := 10
	for i := 0; i < appendCount; i++ {
		if _, err := lm.Append(largeRecord); err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	// Verify we can iterate over all records
	iter, err := lm.Iterator()
	if err != nil {
		t.Fatalf("failed to create iterator: %v", err)
	}

	count := 0
	for rec, err := range iter {
		if err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		if len(rec) != len(largeRecord) {
			t.Errorf("record %d: length mismatch, expected %d, got %d", count, len(largeRecord), len(rec))
		}
		count++
	}

	if count != appendCount {
		t.Errorf("expected %d records, got %d", appendCount, count)
	}

	// Verify we created multiple blocks by checking file size
	logPath := filepath.Join(dirFile.Name(), "test.log")
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("failed to stat log file: %v", err)
	}

	expectedBlocks := 1
	if info.Size() > int64(blockSize) {
		expectedBlocks = int(info.Size()) / blockSize
	}

	if expectedBlocks < 2 {
		t.Logf("warning: expected multiple blocks but got %d (file size: %d)", expectedBlocks, info.Size())
	}
}

func TestLogManagerEmptyLog(t *testing.T) {
	dir, cleanup := setupTestDir(t)
	defer cleanup()

	fm, err := dbfile.NewFileManager(dir, 400)
	if err != nil {
		t.Fatalf("failed to create file manager: %v", err)
	}

	lm, err := dblog.NewLogManager(fm, "empty.log")
	if err != nil {
		t.Fatalf("failed to create log manager: %v", err)
	}

	// Get iterator on empty log
	iter, err := lm.Iterator()
	if err != nil {
		t.Fatalf("failed to create iterator: %v", err)
	}

	count := 0
	for _, err := range iter {
		if err != nil {
			t.Fatalf("iterator error: %v", err)
		}
		count++
	}

	if count != 0 {
		t.Errorf("expected 0 records in empty log, got %d", count)
	}
}

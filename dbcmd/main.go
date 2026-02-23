package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbplan"
	"github.com/teru01/simpledb-go/dbtx"
)

func main() {
	dirName := getEnvOrDefault("BASE_DIR", filepath.Join(os.Getenv("PWD"), ".dbdata"))
	blockSize := getEnvIntOrDefault("BLOCK_SIZE", 4000)
	bufferSize := getEnvIntOrDefault("BUFFER_SIZE", 100)
	totalRecords := getEnvIntOrDefault("TOTAL_RECORDS", 100_000)
	withIndex := getEnvOrDefault("WITH_INDEX", "false") == "true"

	if err := os.MkdirAll(dirName, 0755); err != nil {
		slog.Error("failed to create base dir", "dir", dirName, "error", err)
		os.Exit(1)
	}

	f, err := os.Open(dirName)
	if err != nil {
		slog.Error("failed to open dir", "error", err)
		os.Exit(1)
	}
	fm, err := dbfile.NewFileManager(f, blockSize)
	if err != nil {
		slog.Error("failed to create file manager", "error", err)
		os.Exit(1)
	}
	lm, err := dblog.NewLogManager(fm, "log.log")
	if err != nil {
		slog.Error("failed to create log manager", "error", err)
		os.Exit(1)
	}
	bm := dbbuffer.NewBufferManager(fm, lm, bufferSize)

	ctx := context.Background()

	tx, err := dbtx.NewTransaction(fm, lm, bm)
	if err != nil {
		slog.Error("failed to create transaction", "error", err)
		os.Exit(1)
	}
	isNew := fm.IsNew()
	if !isNew {
		if err := tx.Recover(ctx); err != nil {
			slog.Error("failed to recover", "error", err)
			os.Exit(1)
		}
	}
	mm, err := dbmetadata.NewMetadataManager(ctx, isNew, tx)
	if err != nil {
		slog.Error("failed to create metadata manager", "error", err)
		os.Exit(1)
	}
	if err := tx.Commit(); err != nil {
		slog.Error("failed to commit init", "error", err)
		os.Exit(1)
	}

	qp := dbplan.NewQueryPlanner(mm)
	up := dbplan.NewIndexUpdatePlanner(mm)
	planner := dbplan.NewPlanner(qp, up)

	// CREATE TABLE
	tx, err = dbtx.NewTransaction(fm, lm, bm)
	if err != nil {
		slog.Error("failed to create transaction", "error", err)
		os.Exit(1)
	}
	if _, err := planner.ExecuteUpdate(ctx, `CREATE TABLE students (id INT, name VARCHAR(10), class VARCHAR(1))`, tx); err != nil {
		slog.Error("failed to create table", "error", err)
		os.Exit(1)
	}
	if err := tx.Commit(); err != nil {
		slog.Error("failed to commit create table", "error", err)
		os.Exit(1)
	}
	slog.Info("created table students")

	// CREATE INDEX on id if requested
	if withIndex {
		tx, err = dbtx.NewTransaction(fm, lm, bm)
		if err != nil {
			slog.Error("failed to create transaction", "error", err)
			os.Exit(1)
		}
		if _, err := planner.ExecuteUpdate(ctx, `CREATE INDEX idx_students_id ON students (id)`, tx); err != nil {
			slog.Error("failed to create index", "error", err)
			os.Exit(1)
		}
		if err := tx.Commit(); err != nil {
			slog.Error("failed to commit create index", "error", err)
			os.Exit(1)
		}
		slog.Info("created index idx_students_id on students(id)")
	}

	// INSERT records
	classes := []string{"A", "B", "C", "D", "E", "F"}
	names := []string{"sheep", "goat", "cow", "cat", "dog", "bird", "fish", "frog", "lion", "bear"}

	batchSize := 1000
	for i := 0; i < totalRecords; i += batchSize {
		tx, err = dbtx.NewTransaction(fm, lm, bm)
		if err != nil {
			slog.Error("failed to create transaction", "error", err)
			os.Exit(1)
		}
		end := i + batchSize
		if end > totalRecords {
			end = totalRecords
		}
		for j := i; j < end; j++ {
			name := names[rand.IntN(len(names))]
			class := classes[rand.IntN(len(classes))]
			sql := fmt.Sprintf(`INSERT INTO students (id, name, class) VALUES (%d, "%s", "%s")`, j+1, name, class)
			if _, err := planner.ExecuteUpdate(ctx, sql, tx); err != nil {
				slog.Error("failed to insert", "id", j+1, "error", err)
				os.Exit(1)
			}
		}
		if err := tx.Commit(); err != nil {
			slog.Error("failed to commit batch", "error", err)
			os.Exit(1)
		}
		if end%10000 == 0 {
			slog.Info("inserted records", "count", end)
		}
	}

	slog.Info("done", "total", totalRecords)
	f.Close()
}

func getEnvOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvIntOrDefault(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Error("invalid env value", "key", key, "value", v, "error", err)
		os.Exit(1)
	}
	return n
}

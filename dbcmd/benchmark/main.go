package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/teru01/simpledb-go/dbbuffer"
	"github.com/teru01/simpledb-go/dbfile"
	"github.com/teru01/simpledb-go/dblog"
	"github.com/teru01/simpledb-go/dbmetadata"
	"github.com/teru01/simpledb-go/dbplan"
	"github.com/teru01/simpledb-go/dbtx"
)

type benchConfig struct {
	blockSize  int
	bufferSize int
	records    int
	iterations int
	dataDir    string
}

func main() {
	prepare := flag.Bool("prepare", false, "prepare data only (create table and insert records into a fresh database)")
	index := flag.Bool("index", false, "create index only (on existing data)")
	sel := flag.Bool("select", false, "run 100 SELECT queries by id on existing data")
	flag.Parse()

	cfg := benchConfig{
		blockSize:  getEnvIntOrDefault("BLOCK_SIZE", 4000),
		bufferSize: getEnvIntOrDefault("BUFFER_SIZE", 100),
		records:    getEnvIntOrDefault("RECORDS", 10000),
		iterations: getEnvIntOrDefault("ITERATIONS", 100),
		dataDir:    os.Getenv("DATA_DIR"),
	}

	mode := "all"
	if *prepare {
		mode = "prepare"
	} else if *index {
		mode = "index"
	} else if *sel {
		mode = "select"
	}

	dirName := cfg.dataDir
	cleanUp := func() {}

	switch mode {
	case "select":
		if dirName == "" {
			slog.Error("DATA_DIR is required for -select")
			os.Exit(1)
		}
	case "prepare":
		if dirName == "" {
			slog.Error("DATA_DIR is required for -prepare")
			os.Exit(1)
		}
		if err := os.RemoveAll(dirName); err != nil {
			slog.Error("failed to remove data dir", "error", err)
			os.Exit(1)
		}
		if err := os.MkdirAll(dirName, 0755); err != nil {
			slog.Error("failed to create data dir", "error", err)
			os.Exit(1)
		}
	case "index":
		if dirName == "" {
			slog.Error("DATA_DIR is required for -index")
			os.Exit(1)
		}
	case "all":
		if dirName == "" {
			var err error
			dirName, err = os.MkdirTemp("", "simpledb-bench-*")
			if err != nil {
				slog.Error("failed to create temp dir", "error", err)
				os.Exit(1)
			}
			cleanUp = func() { os.RemoveAll(dirName) }
		} else {
			if err := os.RemoveAll(dirName); err != nil {
				slog.Error("failed to remove data dir", "error", err)
				os.Exit(1)
			}
			if err := os.MkdirAll(dirName, 0755); err != nil {
				slog.Error("failed to create data dir", "error", err)
				os.Exit(1)
			}
		}
	}
	defer cleanUp()

	fm, lm, bm, err := initDB(dirName, cfg)
	if err != nil {
		slog.Error("failed to init db", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()
	planner, err := setupPlanner(ctx, fm, lm, bm)
	if err != nil {
		slog.Error("failed to setup planner", "error", err)
		os.Exit(1)
	}

	fmt.Printf("=== SimpleDB Benchmark (mode=%s) ===\n", mode)
	fmt.Printf("records=%d  iterations=%d  blockSize=%d  bufferSize=%d  dataDir=%s\n\n", cfg.records, cfg.iterations, cfg.blockSize, cfg.bufferSize, dirName)

	switch mode {
	case "prepare":
		benchCreateTable(ctx, fm, lm, bm, planner)
		benchInsert(ctx, fm, lm, bm, planner, cfg.records)
	case "index":
		benchCreateIndex(ctx, fm, lm, bm, planner)
		benchSelectByID(ctx, fm, lm, bm, planner, cfg.records, cfg.iterations, "by id")
	case "select":
		profName := os.Getenv("PROF_NAME")
		if profName == "" {
			profName = "cpu.prof"
		} else {
			profName = "cpu-" + profName + ".prof"
		}
		f, err := os.Create(profName)
		if err != nil {
			slog.Error("failed to create cpu profile", "error", err)
			os.Exit(1)
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
		benchSelectByID(ctx, fm, lm, bm, planner, cfg.records, cfg.iterations, "by id")
	case "all":
		benchCreateTable(ctx, fm, lm, bm, planner)
		benchInsert(ctx, fm, lm, bm, planner, cfg.records)
		benchSelectFullScan(ctx, fm, lm, bm, planner)
		benchSelectWithWhere(ctx, fm, lm, bm, planner)
		benchUpdate(ctx, fm, lm, bm, planner)
		benchDelete(ctx, fm, lm, bm, planner)

		fmt.Printf("\n--- Index (id) ---\n\n")

		benchSelectByID(ctx, fm, lm, bm, planner, cfg.records, cfg.iterations, "without index")
		benchCreateIndex(ctx, fm, lm, bm, planner)
		benchSelectByID(ctx, fm, lm, bm, planner, cfg.records, cfg.iterations, "with index")
	}
}

func initDB(dirName string, cfg benchConfig) (*dbfile.FileManager, *dblog.LogManager, *dbbuffer.BufferManager, error) {
	f, err := os.Open(dirName)
	if err != nil {
		return nil, nil, nil, err
	}
	fm, err := dbfile.NewFileManager(f, cfg.blockSize)
	if err != nil {
		return nil, nil, nil, err
	}
	lm, err := dblog.NewLogManager(fm, "log.log")
	if err != nil {
		return nil, nil, nil, err
	}
	bm := dbbuffer.NewBufferManager(fm, lm, cfg.bufferSize)
	return fm, lm, bm, nil
}

func setupPlanner(ctx context.Context, fm *dbfile.FileManager, lm *dblog.LogManager, bm *dbbuffer.BufferManager) (*dbplan.Planner, error) {
	tx, err := dbtx.NewTransaction(fm, lm, bm)
	if err != nil {
		return nil, err
	}
	mm, err := dbmetadata.NewMetadataManager(ctx, fm.IsNew(), tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	qp := dbplan.NewQueryPlanner(mm)
	up := dbplan.NewIndexUpdatePlanner(mm)
	return dbplan.NewPlanner(qp, up), nil
}

func execBench(ctx context.Context, fm *dbfile.FileManager, lm *dblog.LogManager, bm *dbbuffer.BufferManager, fn func(ctx context.Context, tx *dbtx.Transaction) error) error {
	tx, err := dbtx.NewTransaction(fm, lm, bm)
	if err != nil {
		return fmt.Errorf("create transaction: %w", err)
	}
	if err := fn(ctx, tx); err != nil {
		tx.Rollback(ctx)
		return err
	}
	return tx.Commit()
}

func benchCreateTable(ctx context.Context, fm *dbfile.FileManager, lm *dblog.LogManager, bm *dbbuffer.BufferManager, planner *dbplan.Planner) {
	start := time.Now()
	if err := execBench(ctx, fm, lm, bm, func(ctx context.Context, tx *dbtx.Transaction) error {
		_, err := planner.ExecuteUpdate(ctx, `CREATE TABLE bench (id INT, name VARCHAR(10), class VARCHAR(1))`, tx)
		return err
	}); err != nil {
		slog.Error("CREATE TABLE failed", "error", err)
		return
	}
	printResult("CREATE TABLE", 1, time.Since(start))
}

func benchInsert(ctx context.Context, fm *dbfile.FileManager, lm *dblog.LogManager, bm *dbbuffer.BufferManager, planner *dbplan.Planner, totalRecords int) {
	classes := []string{"A", "B", "C", "D", "E", "F"}
	names := []string{"sheep", "goat", "cow", "cat", "dog", "bird", "fish", "frog", "lion", "bear"}

	batchSize := 1000
	start := time.Now()
	for i := 0; i < totalRecords; i += batchSize {
		tx, err := dbtx.NewTransaction(fm, lm, bm)
		if err != nil {
			slog.Error("failed to create transaction", "error", err)
			return
		}
		end := i + batchSize
		if end > totalRecords {
			end = totalRecords
		}
		for j := i; j < end; j++ {
			name := names[rand.IntN(len(names))]
			class := classes[rand.IntN(len(classes))]
			sql := fmt.Sprintf(`INSERT INTO bench (id, name, class) VALUES (%d, "%s", "%s")`, j+1, name, class)
			if _, err := planner.ExecuteUpdate(ctx, sql, tx); err != nil {
				slog.Error("failed to insert", "error", err)
				return
			}
		}
		if err := tx.Commit(); err != nil {
			slog.Error("failed to commit", "error", err)
			return
		}
		if end%10000 == 0 || end == totalRecords {
			fmt.Printf("  INSERT progress: %d / %d\n", end, totalRecords)
		}
	}
	printResult("INSERT", totalRecords, time.Since(start))
}

func benchSelectFullScan(ctx context.Context, fm *dbfile.FileManager, lm *dblog.LogManager, bm *dbbuffer.BufferManager, planner *dbplan.Planner) {
	start := time.Now()
	count := 0
	if err := execBench(ctx, fm, lm, bm, func(ctx context.Context, tx *dbtx.Transaction) error {
		return scanQuery(ctx, planner, tx, `SELECT id, name, class FROM bench`, &count)
	}); err != nil {
		slog.Error("SELECT full scan failed", "error", err)
		return
	}
	printResult(fmt.Sprintf("SELECT full scan (%d rows)", count), 1, time.Since(start))
}

func benchSelectWithWhere(ctx context.Context, fm *dbfile.FileManager, lm *dblog.LogManager, bm *dbbuffer.BufferManager, planner *dbplan.Planner) {
	start := time.Now()
	count := 0
	if err := execBench(ctx, fm, lm, bm, func(ctx context.Context, tx *dbtx.Transaction) error {
		return scanQuery(ctx, planner, tx, `SELECT id, name, class FROM bench WHERE name = "goat"`, &count)
	}); err != nil {
		slog.Error("SELECT WHERE failed", "error", err)
		return
	}
	printResult(fmt.Sprintf("SELECT WHERE name=\"goat\" (%d rows)", count), 1, time.Since(start))
}

func benchUpdate(ctx context.Context, fm *dbfile.FileManager, lm *dblog.LogManager, bm *dbbuffer.BufferManager, planner *dbplan.Planner) {
	start := time.Now()
	n := 0
	if err := execBench(ctx, fm, lm, bm, func(ctx context.Context, tx *dbtx.Transaction) error {
		var err error
		n, err = planner.ExecuteUpdate(ctx, `UPDATE bench SET class = "Z" WHERE name = "cat"`, tx)
		return err
	}); err != nil {
		slog.Error("UPDATE failed", "error", err)
		return
	}
	printResult(fmt.Sprintf("UPDATE (%d rows affected)", n), 1, time.Since(start))
}

func benchDelete(ctx context.Context, fm *dbfile.FileManager, lm *dblog.LogManager, bm *dbbuffer.BufferManager, planner *dbplan.Planner) {
	start := time.Now()
	n := 0
	if err := execBench(ctx, fm, lm, bm, func(ctx context.Context, tx *dbtx.Transaction) error {
		var err error
		n, err = planner.ExecuteUpdate(ctx, `DELETE FROM bench WHERE name = "frog"`, tx)
		return err
	}); err != nil {
		slog.Error("DELETE failed", "error", err)
		return
	}
	printResult(fmt.Sprintf("DELETE (%d rows affected)", n), 1, time.Since(start))
}

func benchCreateIndex(ctx context.Context, fm *dbfile.FileManager, lm *dblog.LogManager, bm *dbbuffer.BufferManager, planner *dbplan.Planner) {
	fmt.Println("  CREATE INDEX progress: started")
	start := time.Now()
	if err := execBench(ctx, fm, lm, bm, func(ctx context.Context, tx *dbtx.Transaction) error {
		_, err := planner.ExecuteUpdate(ctx, `CREATE INDEX idx_bench_id ON bench (id)`, tx)
		return err
	}); err != nil {
		slog.Error("CREATE INDEX failed", "error", err)
		return
	}
	fmt.Println("  CREATE INDEX progress: done")
	printResult("CREATE INDEX on id", 1, time.Since(start))
}

func benchSelectByID(ctx context.Context, fm *dbfile.FileManager, lm *dblog.LogManager, bm *dbbuffer.BufferManager, planner *dbplan.Planner, records int, iterations int, label string) {
	fm.ResetCounts()
	start := time.Now()
	count := 0
	for i := range iterations {
		id := i%records + 1
		if err := execBench(ctx, fm, lm, bm, func(ctx context.Context, tx *dbtx.Transaction) error {
			return scanQuery(ctx, planner, tx, fmt.Sprintf(`SELECT id, name, class FROM bench WHERE id = %d`, id), &count)
		}); err != nil {
			slog.Error("SELECT failed", "error", err, "label", label)
			return
		}
	}
	printResult(fmt.Sprintf("SELECT %s (%d lookups)", label, iterations), iterations, time.Since(start))
	fmt.Printf("  disk I/O: read=%d  write=%d\n", fm.ReadCount(), fm.WriteCount())
	for file, count := range fm.ReadCountByFile() {
		fmt.Printf("    %s: %d\n", file, count)
	}
}

func scanQuery(ctx context.Context, planner *dbplan.Planner, tx *dbtx.Transaction, sql string, count *int) error {
	plan, err := planner.CreateQueryPlan(ctx, sql, tx)
	if err != nil {
		return err
	}
	scan, err := plan.Open(ctx)
	if err != nil {
		return err
	}
	defer scan.Close(ctx)
	*count = 0
	for {
		ok, err := scan.Next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		*count++
	}
	return nil
}

func printResult(label string, ops int, elapsed time.Duration) {
	if ops == 1 {
		fmt.Printf("%-50s %12s\n", label, elapsed.Round(time.Millisecond))
	} else {
		opsPerSec := float64(ops) / elapsed.Seconds()
		fmt.Printf("%-50s %12s  (%.0f ops/sec)\n", label, elapsed.Round(time.Millisecond), opsPerSec)
	}
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

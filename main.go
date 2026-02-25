package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/chzyer/readline"
	"github.com/teru01/simpledb-go/dbexecutor"
	"github.com/teru01/simpledb-go/dbserver"
)

func main() {
	slog.Info("starting simpledb...")

	dirName := getEnvOrDefault("BASE_DIR", filepath.Join(os.Getenv("PWD"), ".dbdata"))
	blockSize := getEnvIntOrDefault("BLOCK_SIZE", 4000)
	bufferSize := getEnvIntOrDefault("BUFFER_SIZE", 100)

	if err := os.MkdirAll(dirName, 0755); err != nil {
		slog.Error("failed to create base dir", "dir", dirName, "error", err)
		os.Exit(1)
	}

	db, cleanup, err := dbexecutor.NewSimpleDB(dirName, blockSize, bufferSize)
	if err != nil {
		slog.Error("failed to create simpledb", "error", err)
		os.Exit(1)
	}
	defer cleanup()

	ctx := context.Background()
	if err := db.Init(ctx); err != nil {
		slog.Error("failed to initialize simpledb", "error", err)
		os.Exit(1)
	}

	slog.Info("simpledb started", "dir", dirName, "blockSize", blockSize, "bufferSize", bufferSize)

	mode := getEnvOrDefault("MODE", "repl")
	switch mode {
	case "server":
		addr := getEnvOrDefault("LISTEN_ADDR", ":5432")
		if err := dbserver.StartServer(ctx, addr, db); err != nil {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	default:
		runREPL(ctx, db, dirName)
	}
}

func runREPL(ctx context.Context, db *dbexecutor.SimpleDB, dirName string) {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:      "> ",
		HistoryFile: filepath.Join(dirName, ".simpledb_history"),
	})
	if err != nil {
		slog.Error("failed to create readline", "error", err)
		os.Exit(1)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err != nil {
			if err == io.EOF || err == readline.ErrInterrupt {
				break
			}
			slog.Error("reading input", "error", err)
			break
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		result, err := db.Execute(ctx, line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			continue
		}
		printResult(result)
	}
}

func printResult(r *dbexecutor.ExecuteResult) {
	if len(r.Fields) == 0 {
		fmt.Println(r.Tag)
		return
	}
	// calculate column widths
	widths := make([]int, len(r.Fields))
	for i, f := range r.Fields {
		widths[i] = len(f)
	}
	for _, row := range r.Rows {
		for i, v := range row {
			if len(v) > widths[i] {
				widths[i] = len(v)
			}
		}
	}
	// header
	header := make([]string, len(r.Fields))
	for i, f := range r.Fields {
		header[i] = padRight(f, widths[i])
	}
	fmt.Println(strings.Join(header, " | "))
	// separator
	sep := make([]string, len(r.Fields))
	for i, w := range widths {
		sep[i] = strings.Repeat("-", w)
	}
	fmt.Println(strings.Join(sep, "-+-"))
	// rows
	for _, row := range r.Rows {
		cols := make([]string, len(r.Fields))
		for i, v := range row {
			cols[i] = padRight(v, widths[i])
		}
		fmt.Println(strings.Join(cols, " | "))
	}
	fmt.Printf("(%d rows)\n", len(r.Rows))
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
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

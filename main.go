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

	replMode := getEnvOrDefault("REPL_MODE", "true")
	if replMode != "true" {
		return
	}

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

		if err := db.Execute(ctx, line); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
	}
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

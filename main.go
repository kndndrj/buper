package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

type hash [16]byte

type sourceFile struct {
	Path             string
	Size             int64
	Mtime            time.Time
	ProcessedCommand string

	ProcessedHash hash
}

type outputFile struct {
	Path  string
	Size  int64
	Mtime time.Time

	Hash hash
}

func main() {
	os.Exit(m())
}

func m() int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := parseConfig()
	if err != nil {
		fmt.Println("failed parsing flags:", err)
		return 1
	}

	if err := run(ctx, cfg); err != nil {
		if errors.Is(err, errLocked) {
			fmt.Println("Cache is locked by another process")
		} else if errors.Is(err, context.Canceled) {
			fmt.Println("Canceled")
		}
		fmt.Println("Error:", err)
		return 1
	}

	return 0
}

func run(ctx context.Context, cfg *config) error {
	cache, err := newCache(cfg.CachePath())
	if err != nil {
		return fmt.Errorf("failed creating cache: %w", err)
	}
	defer func() {
		if err := cache.Close(); err != nil {
			fmt.Println("failed closing cache:", err)
		}
	}()
	return process(ctx, cfg, cache)
}

// process contains the core logic.
func process(ctx context.Context, cfg *config, cache *cache) error {
	if err := createDirs(cfg); err != nil {
		return fmt.Errorf("failed creating directories: %w", err)
	}

	if err := cache.Invalidate(ctx); err != nil {
		return fmt.Errorf("failed invalidating cache: %w", err)
	}

	fmt.Println("event: updating output cache")

	if err := updateOutputCache(ctx, cache, cfg); err != nil {
		return fmt.Errorf("failed updating output directory cache: %w", err)
	}

	fmt.Println("event: processing sources")

	if err := processSources(ctx, cache, cfg); err != nil {
		return fmt.Errorf("failed processing sources: %w", err)
	}

	if err := cache.CleanInvalid(ctx); err != nil {
		return fmt.Errorf("failed cleaning cache: %w", err)
	}

	fmt.Println("event: cleaning dump directory")

	if err := cleanDumpDirectory(ctx, cache, cfg); err != nil {
		return fmt.Errorf("failed cleaning dump directory: %w", err)
	}

	if err := cleanup(cfg); err != nil {
		return fmt.Errorf("failed cleaning up: %w", err)
	}

	return nil
}

// updateOutputCache updates cache of a directory holding output files.
func updateOutputCache(ctx context.Context, cache *cache, cfg *config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for f, err := range find(cfg.OutputDirectory, cfg.Extensions) {
		if err != nil {
			return fmt.Errorf("find error: %w", err)
		}

		if err := updateOutputCacheFile(ctx, cache, f); err != nil {
			return fmt.Errorf("failed updating cache file: %w", err)
		}
	}

	return nil
}

func updateOutputCacheFile(ctx context.Context, cache *cache, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed stating file: %w", err)
	}

	valid, err := cache.CheckAndValidateOutputFile(ctx, path, info.Size(), info.ModTime())
	if err != nil {
		return fmt.Errorf("failed checking file: %w", err)
	}

	if !valid {
		hash, err := md5Hash(path)
		if err != nil {
			return fmt.Errorf("failed hasing: %w", err)
		}

		if err := cache.InsertOutputFile(ctx, &outputFile{
			Path:  path,
			Size:  info.Size(),
			Mtime: info.ModTime(),
			Hash:  hash,
		}); err != nil {
			return fmt.Errorf("failed inserting: %w", err)
		}
	}

	return nil
}

// processSources processes source files to produce new output files.
func processSources(ctx context.Context, cache *cache, cfg *config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, dir := range cfg.SourceDirectories {
		for f, err := range find(dir, cfg.Extensions) {
			if err != nil {
				return fmt.Errorf("find error: %w", err)
			}

			if err := processSource(ctx, cache, f, cfg); err != nil {
				return fmt.Errorf("failed processing source: %w", err)
			}
		}
	}

	return nil
}

func processSource(ctx context.Context, cache *cache, path string, cfg *config) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed stating file: %w", err)
	}

	processedHash, valid, err := cache.CheckProcessedHashAndValidateSourceFile(ctx, path, info.Size(), info.ModTime(), cfg.Command)
	if err != nil {
		return fmt.Errorf("failed checking file cache: %w", err)
	}

	var processedPath string
	if !valid {
		out, err := applyCommand(ctx, path, cfg)
		if err != nil {
			return fmt.Errorf("failed applying command: %w", err)
		}

		hsh, err := md5Hash(out)
		if err != nil {
			return fmt.Errorf("failed hashing file: %w", err)
		}

		processedPath = out
		processedHash = hsh

		if err := cache.InsertSourceFile(ctx, &sourceFile{
			Path:             path,
			Size:             info.Size(),
			Mtime:            info.ModTime(),
			ProcessedCommand: cfg.Command,
			ProcessedHash:    processedHash,
		}); err != nil {
			return fmt.Errorf("failed inserting source to cache: %w", err)
		}
	}

	exists, err := cache.CheckOutputFileHashExists(ctx, processedHash)
	if err != nil {
		return fmt.Errorf("failed checking for output file hash: %w", err)
	}

	if exists {
		return nil
	}

	if processedPath == "" {
		processedPath, err = applyCommand(ctx, path, cfg)
		if err != nil {
			return fmt.Errorf("failed applying command: %w", err)
		}
	}

	// Move the file to dumping directory.
	moved := filepath.Join(cfg.DumpDirectory(), filepath.Base(path))
	moved, err = move(processedPath, moved)
	if err != nil {
		return fmt.Errorf("failed moving file: %w", err)
	}

	info, err = os.Stat(moved)
	if err != nil {
		return fmt.Errorf("failed stating output: %w", err)
	}

	// Store to cache.
	if err := cache.InsertOutputFile(ctx, &outputFile{
		Path:  moved,
		Size:  info.Size(),
		Mtime: info.ModTime(),
		Hash:  processedHash,
	}); err != nil {
		return fmt.Errorf("failed inserting output to cache: %w", err)
	}

	fmt.Println("processed:", path, "=>", moved)

	return nil
}

// cleanDumpDirectory removes files from the dump directory if it finds them elsewhere.
func cleanDumpDirectory(ctx context.Context, cache *cache, cfg *config) error {
	dumpDirectory := filepath.Join(cfg.OutputDirectory, cfg.DumpSubdirectory)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for f, err := range find(dumpDirectory, cfg.Extensions) {
		if err != nil {
			return fmt.Errorf("find error: %w", err)
		}

		if err := cleanDumpFile(ctx, cache, f); err != nil {
			return fmt.Errorf("failed cleaning dump file: %w", err)
		}
	}

	return nil
}

// cleanDumpFile removes a file from the dump directory, if a file with the same contents already exists elsewhere.
func cleanDumpFile(ctx context.Context, cache *cache, path string) error {
	duplicateExists, err := cache.CheckOutputFileDuplicates(ctx, path)
	if err != nil {
		return fmt.Errorf("failed checking hash: %w", err)
	}

	if !duplicateExists {
		return nil
	}

	// Exists.

	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed removing duplicated dump file: %w", err)
	}

	if err := cache.RemoveOutputFile(ctx, path); err != nil {
		return fmt.Errorf("failed removing from cache: %w", err)
	}

	fmt.Println("cleaned:", path)

	return nil
}

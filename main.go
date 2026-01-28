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

	"golang.org/x/sync/errgroup"
)

type config struct {
	// Directories to fetch sources from.
	SourceDirectories []string
	// Directory holding all outputs.
	OutputDirectory string
	// Subdirectory of OutputDirectory, that is used for dumping new outputs.
	DumpSubdirectory string
	// Command to apply to each source.
	Command string
	// File extensions to search for.
	Extensions []string
	// Number of jobs to run simultaniously.
	NumJobs int
	// Cache location.
	CachePath string
}

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
	cfg := &config{}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		if errors.Is(err, errLocked) {
			fmt.Println("Cache is locked by another process")
			return 1
		}
	}

	fmt.Println("Success")
	return 0
}

func run(ctx context.Context, cfg *config) error {
	cache, err := newCache(cfg.CachePath)
	if err != nil {
		return fmt.Errorf("failed creating cache: %w", err)
	}
	defer func() { fmt.Println("failed closing cache:", cache.Close()) }()

	if err := cache.Invalidate(ctx); err != nil {
		return fmt.Errorf("failed invalidating cache: %w", err)
	}

	if err = updateOutputCache(ctx, cache, cfg); err != nil {
		return fmt.Errorf("failed updating output directory cache: %w", err)
	}

	if err := processSources(ctx, cache, cfg); err != nil {
		return fmt.Errorf("failed processing sources: %w", err)
	}

	if err := cache.CleanInvalid(ctx); err != nil {
		return fmt.Errorf("failed cleaning cache: %w", err)
	}

	return nil
}

// updateOutputCache updates cache of a directory holding output files.
func updateOutputCache(ctx context.Context, cache *cache, cfg *config) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.NumJobs)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for f, err := range find(cfg.OutputDirectory, cfg.Extensions) {
		if err != nil {
			return fmt.Errorf("find error: %w", err)
		}

		g.Go(func() error {
			return updateOutputCacheFile(ctx, cache, f)
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed processing sources: %w", err)
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

func processSources(ctx context.Context, cache *cache, cfg *config) error {
	dumpDirectory := filepath.Join(cfg.OutputDirectory, cfg.DumpSubdirectory)

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(cfg.NumJobs)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, dir := range cfg.SourceDirectories {
		for f, err := range find(dir, cfg.Extensions) {
			if err != nil {
				return fmt.Errorf("find error: %w", err)
			}

			g.Go(func() error {
				return processSource(ctx, cache, f, cfg.Command, dumpDirectory)
			})
		}
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed processing sources: %w", err)
	}

	return nil
}

func processSource(ctx context.Context, cache *cache, path string, command string, dumpDirectory string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed stating file: %w", err)
	}

	processedHash, valid, err := cache.CheckProcessedHashAndValidateSourceFile(ctx, path, info.Size(), info.ModTime(), command)
	if err != nil {
		return fmt.Errorf("failed checking file cache: %w", err)
	}

	var outputPath string
	if !valid {
		out, err := applyCommand(ctx, path, command)
		if err != nil {
			return fmt.Errorf("failed applying command: %w", err)
		}

		hsh, err := md5Hash(out)
		if err != nil {
			return fmt.Errorf("failed hashing file: %w", err)
		}

		outputPath = out
		processedHash = hsh
	}

	exists, err := cache.CheckOutputFileHashExists(ctx, processedHash)
	if err != nil {
		return fmt.Errorf("failed checking for output file hash: %w", err)
	}

	if exists {
		// Derived output file already exists, do nothing.
		return nil
	}

	// Derived file doesn't exist.

	if err := cache.InsertSourceFile(ctx, &sourceFile{
		Path:             path,
		Size:             info.Size(),
		Mtime:            info.ModTime(),
		ProcessedCommand: command,
		ProcessedHash:    processedHash,
	}); err != nil {
		return fmt.Errorf("failed inserting to cache: %w", err)
	}

	// Move the file to dumping directory.
	moved := filepath.Join(dumpDirectory, filepath.Base(path))
	if err := os.Rename(outputPath, moved); err != nil {
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

	return nil
}

package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bmatcuk/doublestar/v4"
)

// find recursively lists the files with given extensions within a directory.
func find(path string, exclusions []string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err // Stop iteration.
			}

			if d.IsDir() {
				return nil // Skip directories.
			}

			if matchPatterns(path, exclusions) {
				return nil // Skip exclude pattern.
			}

			if !yield(path, nil) {
				return fs.SkipAll
			}

			return nil
		})
		if err != nil {
			yield("", err)
		}
	}
}

// matchPatterns checks if the path matches any of the provided patterns.
func matchPatterns(path string, patterns []string) bool {
	for _, pattern := range patterns {
		if doublestar.MatchUnvalidated(pattern, path) {
			return true
		}
	}

	return false
}

func md5Hash(path string) (hash, error) {
	var result hash

	path = filepath.Clean(path)

	f, err := os.Open(path)
	if err != nil {
		return result, fmt.Errorf("open file: %w", err)
	}
	defer f.Close() //nolint:errcheck

	hasher := md5.New()
	if _, err := io.Copy(hasher, f); err != nil {
		return result, fmt.Errorf("hash file: %w", err)
	}

	copy(result[:], hasher.Sum(nil))
	return result, nil
}

func createDirs(cfg *config) error {
	if err := os.MkdirAll(cfg.TempDir(), 0o750); err != nil {
		return fmt.Errorf("creating tempdir: %w", err)
	}

	dumpdir := filepath.Join(cfg.OutputDirectory, cfg.DumpSubdirectory)
	if err := os.MkdirAll(dumpdir, 0o750); err != nil {
		return fmt.Errorf("creating output and dump dirs: %w", err)
	}

	return nil
}

func cleanup(cfg *config) error {
	if err := os.RemoveAll(cfg.TempDir()); err != nil {
		return fmt.Errorf("failed removing tempdir: %w", err)
	}

	return nil
}

// applyCommand applies command to the source path and produces an output at output path.
func applyCommand(ctx context.Context, sourcePath string, cfg *config) (outputPath string, err error) {
	rand := strconv.Itoa(int(time.Now().UnixNano()))

	// NOTE: output path must preserve the extension, since some programs (like ffmpeg) depend on it.
	ext := filepath.Ext(sourcePath)
	fname := filepath.Base(sourcePath)
	fname = strings.TrimSuffix(fname, ext)
	outputPath = filepath.Join(cfg.TempDir(), fname+"-"+rand+ext)

	command := strings.ReplaceAll(cfg.Command, "$in", `"`+sourcePath+`"`)
	command = strings.ReplaceAll(command, "$out", `"`+outputPath+`"`)

	// Execute using sh -c to allow piping and shell features.
	cmd := exec.CommandContext(ctx, "sh", "-c", command) //nolint:gosec

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("command failed: %w\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	return outputPath, nil
}

var movemu sync.Mutex

// move moves a file from src to dst.
// operations are synchronized.
func move(src, dst string) (newdst string, err error) {
	movemu.Lock()
	defer movemu.Unlock()

	// Check if a destination already exists and add a suffix if it does.
	var dstname func(iteration int) (string, error)
	dstname = func(iteration int) (string, error) {
		ext := filepath.Ext(dst)
		base := strings.TrimSuffix(dst, ext)
		target := base + "-" + strconv.Itoa(iteration) + ext
		if iteration == 0 {
			target = dst
		}

		_, err := os.Stat(target)
		if err == nil {
			return dstname(iteration + 1)
		}

		if !errors.Is(err, os.ErrNotExist) {
			// Other error.
			return "", err
		}

		// File doesn't exist.
		return target, nil
	}

	newdst, err = dstname(0)
	if err != nil {
		return "", fmt.Errorf("failed getting destination file: %w", err)
	}

	if err := os.Rename(src, newdst); err != nil {
		return "", fmt.Errorf("failed renaming file: %w", err)
	}

	return newdst, nil
}

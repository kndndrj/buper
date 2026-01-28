package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// find recursively lists the files with given extensions within a directory.
func find(path string, extensions []string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err // Stop iteration.
			}

			if d.IsDir() {
				return nil // Skip directories.
			}

			if hasExtension(path, extensions) {
				if !yield(path, nil) {
					return fs.SkipAll
				}
			}

			return nil
		})
		if err != nil {
			yield("", err)
		}
	}
}

// hasExtension checks if a path has any of these extensions.
func hasExtension(path string, extensions []string) bool {
	for _, ext := range extensions {
		if strings.HasSuffix(path, ext) {
			return true
		}
	}

	return false
}

func md5Hash(path string) (hash, error) {
	var result [16]byte

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

// applyCommand applies command to the source path and produces an output at output path.
func applyCommand(ctx context.Context, sourcePath, command string) (outputPath string, err error) {
	rand := strconv.Itoa(int(time.Now().UnixNano()))
	outputPath = filepath.Join(os.TempDir(), "buper", filepath.Base(sourcePath)+"."+rand)

	command = strings.ReplaceAll(command, "$in", sourcePath)
	command = strings.ReplaceAll(command, "$out", outputPath)

	// Execute using sh -c to allow piping and shell features.
	cmd := exec.CommandContext(ctx, "sh", "-c", command)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("command failed: %w\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}

	return outputPath, nil
}

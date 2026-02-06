package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
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
	// Path patterns to exclude.
	Exclude []string
	// Cache directory location.
	CacheDirectory string
}

func (c *config) CachePath() string {
	return filepath.Join(c.CacheDirectory, "cache.sqlite")
}

func (c *config) TempDir() string {
	return filepath.Join(c.CacheDirectory, "tmp")
}

func (c *config) DumpDirectory() string {
	return filepath.Join(c.OutputDirectory, c.DumpSubdirectory)
}

func parseConfig() (*config, error) {
	hash := flagHash()
	defaultCacheDir := filepath.Join(os.TempDir(), "buper-cache", hash)
	defaultCacheDirDisplay := filepath.Join(os.TempDir(), "buper-cache", "<hash>")
	if dir, err := os.UserCacheDir(); err == nil {
		defaultCacheDir = filepath.Join(dir, "buper", hash)
		defaultCacheDirDisplay = filepath.Join(dir, "buper", "<hash>")
	}

	sourceDirsArg := flag.String("source-dirs", "", "(required) Comma-separated list of source directory paths.")
	outputDirArg := flag.String("output-dir", "", "(required) Path to output directory.")
	excludeArg := flag.String("exclude", "", "Comma-separated list of path patterns to exclude.")
	dumpDirArg := flag.String("dump-subdir", "dump", "Subdirectory dumping path relative to output directory.")
	commandArg := flag.String("command", "cp $in $out", "Command applied on sources to produce outputs.")
	cacheDirArg := flag.String("cache-dir", defaultCacheDirDisplay, "Path to cache file.")

	flag.Parse()

	sourceDirs, err := parseSourceDirs(*sourceDirsArg)
	if err != nil {
		return nil, fmt.Errorf("failed parsing source directories: %w", err)
	}
	if *outputDirArg == "" {
		return nil, fmt.Errorf("no output directory provided")
	}

	exclusions, err := parseExclusions(*excludeArg)
	if err != nil {
		return nil, fmt.Errorf("failed parsing exclusions: %w", err)
	}

	cacheDir := *cacheDirArg
	if cacheDir == defaultCacheDirDisplay {
		cacheDir = defaultCacheDir
	}

	return &config{
		SourceDirectories: sourceDirs,
		OutputDirectory:   *outputDirArg,
		DumpSubdirectory:  *dumpDirArg,
		Command:           *commandArg,
		Exclude:           exclusions,
		CacheDirectory:    cacheDir,
	}, nil
}

func parseSourceDirs(raw string) ([]string, error) {
	list := strings.Split(raw, ",")
	if len(list) == 0 {
		return nil, fmt.Errorf("no entry provided")
	}
	for _, l := range list {
		if strings.TrimSpace(l) == "" {
			return nil, fmt.Errorf("empty value in list")
		}
	}
	return list, nil
}

func parseExclusions(raw string) ([]string, error) {
	if raw == "" {
		return []string{}, nil
	}

	list := strings.Split(raw, ",")

	for _, p := range list {
		if !doublestar.ValidatePattern(p) {
			return nil, fmt.Errorf("invalid exclude pattern: %s", p)
		}
	}

	return list, nil
}

func flagHash() string {
	// Make a copy so we don't mutate the original slice.
	sorted := make([]string, len(os.Args))
	copy(sorted, os.Args)

	sort.Strings(sorted)

	h := sha256.New()
	for _, v := range sorted {
		h.Write([]byte(v))
	}

	return hex.EncodeToString(h.Sum(nil))
}

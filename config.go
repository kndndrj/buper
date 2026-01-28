package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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
	// Cache location.
	CachePath string
}

func parseConfig() (*config, error) {
	rand := strconv.Itoa(int(time.Now().UnixNano()))
	defaultCache := filepath.Join(os.TempDir(), "buper-"+rand+".cache")
	defaultCacheDisplay := filepath.Join(os.TempDir(), "buper-<rand>.cache")

	sourceDirsArg := flag.String("sources", "", "(required) Comma-separated list of source directory paths.")
	outputDirArg := flag.String("output", "", "(required) Path to output directory.")
	extensionsArg := flag.String("extensions", "", "(required) Comma-separated list of file extensions to include.")
	dumpDirArg := flag.String("dump", "dump", "Subdirectory dumping path relative to output directory.")
	commandArg := flag.String("command", "cp $in $out", "Command applied on sources to produce outputs.")
	cacheArg := flag.String("cache", defaultCacheDisplay, "Path to cache file.")

	flag.Parse()

	sourceDirs, err := parseList(*sourceDirsArg)
	if err != nil {
		return nil, fmt.Errorf("failed parsing source directories: %w", err)
	}
	if *outputDirArg == "" {
		return nil, fmt.Errorf("no output directory provided")
	}
	extensions, err := parseList(*extensionsArg)
	if err != nil {
		return nil, fmt.Errorf("failed parsing extensions: %w", err)
	}

	cache := *cacheArg
	if cache == defaultCacheDisplay {
		cache = defaultCache
	}

	return &config{
		SourceDirectories: sourceDirs,
		OutputDirectory:   *outputDirArg,
		DumpSubdirectory:  *dumpDirArg,
		Command:           *commandArg,
		Extensions:        extensions,
		CachePath:         cache,
	}, nil
}

func parseList(raw string) ([]string, error) {
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

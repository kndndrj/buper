package main

import (
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	t0 := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(1 * time.Hour)

	src := func(path string, size int64, mtime time.Time, processedCommand string, processedHash string) *sourceFile {
		return &sourceFile{
			Path:             path,
			Size:             size,
			Mtime:            mtime,
			ProcessedCommand: processedCommand,
			ProcessedHash:    hsh(t, processedHash),
		}
	}
	out := func(path string, size int64, mtime time.Time, hash string) *outputFile {
		return &outputFile{
			Path:  path,
			Size:  size,
			Mtime: mtime,
			Hash:  hsh(t, hash),
		}
	}

	cmd := `cp $in $out && printf " - modified" >> $out`

	cases := []struct {
		comment string

		fs    map[string]string
		cache []any // *sourceFile or *outputFile.

		expectFS    map[string]string
		expectCache []any // *sourceFile or *outputFile.
	}{
		{
			comment: "no files",

			fs:    nil,
			cache: nil,

			expectFS:    nil,
			expectCache: nil,
		},
		{
			comment: "no initial output files",

			fs: map[string]string{
				"src/file1.yes":         "content of file 1",
				"src/sub/sub/file2.yes": "content of file 2",
				"src/sub/sub/file3.no":  "content of file 3",
			},
			cache: nil,

			expectFS: map[string]string{
				"src/file1.yes":         "content of file 1",
				"src/sub/sub/file2.yes": "content of file 2",
				"src/sub/sub/file3.no":  "content of file 3",

				"out/dump/file1.yes": "content of file 1 - modified",
				"out/dump/file2.yes": "content of file 2 - modified",
			},
			expectCache: []any{
				src("src/file1.yes", 17, t0, cmd, "2887f195dec56162d856acb79f91c5ef"),
				src("src/sub/sub/file2.yes", 17, t0, cmd, "70505225a9da2655c4d056e1555890b1"),
				out("out/dump/file1.yes", 28, t1, "2887f195dec56162d856acb79f91c5ef"),
				out("out/dump/file2.yes", 28, t1, "70505225a9da2655c4d056e1555890b1"),
			},
		},
		{
			comment: "existing output files",

			fs: map[string]string{
				"src/file1.yes": "content of file 1",
				"src/file2.yes": "content of file 2",
				"src/file3.yes": "content of file 3",

				"out/file1-modified.yes": "content of file 1 - modified",

				"out/dump/file2.yes": "content of file 2 - modified",
			},

			expectFS: map[string]string{
				"src/file1.yes": "content of file 1",
				"src/file2.yes": "content of file 2",
				"src/file3.yes": "content of file 3",

				"out/file1-modified.yes": "content of file 1 - modified",

				"out/dump/file2.yes": "content of file 2 - modified",
				"out/dump/file3.yes": "content of file 3 - modified",
			},
			expectCache: []any{
				src("src/file1.yes", 17, t0, cmd, "2887f195dec56162d856acb79f91c5ef"),
				src("src/file2.yes", 17, t0, cmd, "70505225a9da2655c4d056e1555890b1"),
				src("src/file3.yes", 17, t0, cmd, "4cc0c5390503afacab5c7d38a89ea272"),
				out("out/file1-modified.yes", 28, t0, "2887f195dec56162d856acb79f91c5ef"),
				out("out/dump/file2.yes", 28, t0, "70505225a9da2655c4d056e1555890b1"),
				out("out/dump/file3.yes", 28, t1, "4cc0c5390503afacab5c7d38a89ea272"),
			},
		},
		{
			comment: "dumped output name conflict",

			fs: map[string]string{
				"src/file1.yes":     "content of file 1",
				"src/sub/file1.yes": "content of subfile 1",

				"out/dump/file1.yes": "content of existing file 1",
			},

			expectFS: map[string]string{
				"src/file1.yes":     "content of file 1",
				"src/sub/file1.yes": "content of subfile 1",

				"out/dump/file1.yes":   "content of existing file 1",
				"out/dump/file1-1.yes": "content of file 1 - modified",
				"out/dump/file1-2.yes": "content of subfile 1 - modified",
			},
			expectCache: []any{
				src("src/file1.yes", 17, t0, cmd, "2887f195dec56162d856acb79f91c5ef"),
				src("src/sub/file1.yes", 20, t0, cmd, "de450c2a2b6f253778078d3c4e0f9b43"),
				out("out/dump/file1.yes", 26, t0, "9432cd7cd935269a16bd4d9fb6185b82"),
				out("out/dump/file1-1.yes", 28, t1, "2887f195dec56162d856acb79f91c5ef"),
				out("out/dump/file1-2.yes", 31, t1, "de450c2a2b6f253778078d3c4e0f9b43"),
			},
		},
		{
			comment: "dumped output cleanup",

			fs: map[string]string{
				"out/file1.yes":              "content of file 1 - modified",
				"out/sub/file2-modified.yes": "content of file 2 - modified",

				"out/dump/file1.yes": "content of file 1 - modified", // Should clean.
				"out/dump/file2.yes": "content of file 2 - modified", // Should clean.
				"out/dump/file3.yes": "content of file 3 - modified",
			},

			expectFS: map[string]string{
				"out/file1.yes":              "content of file 1 - modified",
				"out/sub/file2-modified.yes": "content of file 2 - modified",

				"out/dump/file3.yes": "content of file 3 - modified",
			},
			expectCache: []any{
				out("out/file1.yes", 28, t0, "2887f195dec56162d856acb79f91c5ef"),
				out("out/sub/file2-modified.yes", 28, t0, "70505225a9da2655c4d056e1555890b1"),
				out("out/dump/file3.yes", 28, t0, "4cc0c5390503afacab5c7d38a89ea272"),
			},
		},
		{
			comment: "existing cache files",

			fs: map[string]string{
				"src/file1.yes": "content of file 1",
				"src/file2.yes": "content of file 2",
				"src/file3.yes": "content of file 3",
				"src/file4.yes": "content of file 4",
				"src/file5.no":  "content of file 5",

				"out/file1.yes": "content of file 1 - modified",

				"out/dump/file2.yes": "content of file 2 - modified",
				"out/dump/file3.yes": "content of file 3 - modified",
			},
			cache: []any{
				src("src/file1.yes", 17, t0, "wrong command", "2887f195dec56162d856acb79f91c5ef"), // Wrong.
				src("src/file2.yes", 17, time.Time{}, cmd, "70505225a9da2655c4d056e1555890b1"),    // Wrong.
				out("out/file1.yes", 0xBEEF, t0, "2887f195dec56162d856acb79f91c5ef"),              // Wrong.
				out("out/dump/file2.yes", 28, t1, "WRONG"),                                        // Wrong.
				out("out/dump/file3.yes", 28, t1, "4cc0c5390503afacab5c7d38a89ea272"),
			},

			expectFS: map[string]string{
				"src/file1.yes": "content of file 1",
				"src/file2.yes": "content of file 2",
				"src/file3.yes": "content of file 3",
				"src/file4.yes": "content of file 4",
				"src/file5.no":  "content of file 5",

				"out/file1.yes": "content of file 1 - modified",

				"out/dump/file2.yes": "content of file 2 - modified",
				"out/dump/file3.yes": "content of file 3 - modified",
				"out/dump/file4.yes": "content of file 4 - modified",
			},
			expectCache: []any{
				src("src/file1.yes", 17, t0, cmd, "2887f195dec56162d856acb79f91c5ef"),
				src("src/file2.yes", 17, t0, cmd, "70505225a9da2655c4d056e1555890b1"),
				src("src/file3.yes", 17, t0, cmd, "4cc0c5390503afacab5c7d38a89ea272"),
				src("src/file4.yes", 17, t0, cmd, "1820d259e390d30bdaca3839a226b955"),
				out("out/file1.yes", 28, t0, "2887f195dec56162d856acb79f91c5ef"),
				out("out/dump/file2.yes", 28, t0, "70505225a9da2655c4d056e1555890b1"),
				out("out/dump/file3.yes", 28, t0, "4cc0c5390503afacab5c7d38a89ea272"),
				out("out/dump/file4.yes", 28, t1, "1820d259e390d30bdaca3839a226b955"),
			},
		},
		{
			comment: "source cache valid, but output not present",

			fs: map[string]string{
				"src/file1.yes": "content of file 1",
			},
			cache: []any{
				src("src/file1.yes", 17, t0, cmd, "2887f195dec56162d856acb79f91c5ef"),
			},

			expectFS: map[string]string{
				"src/file1.yes": "content of file 1",

				"out/dump/file1.yes": "content of file 1 - modified",
			},
			expectCache: []any{
				src("src/file1.yes", 17, t0, cmd, "2887f195dec56162d856acb79f91c5ef"),
				out("out/dump/file1.yes", 28, t1, "2887f195dec56162d856acb79f91c5ef"),
			},
		},
		{
			comment: "filenames with spaces",

			fs: map[string]string{
				"src/file 1.yes": "content of file 1",
			},

			expectFS: map[string]string{
				"src/file 1.yes": "content of file 1",

				"out/dump/file 1.yes": "content of file 1 - modified",
			},
			expectCache: []any{
				src("src/file 1.yes", 17, t0, cmd, "2887f195dec56162d856acb79f91c5ef"),
				out("out/dump/file 1.yes", 28, t1, "2887f195dec56162d856acb79f91c5ef"),
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.comment, func(t *testing.T) {
			a := require.New(t)

			temp := t.TempDir()
			root := filepath.Join(temp, "root")
			t.Logf("Using temporary dir: %s", temp)

			srcdir := filepath.Join(root, "src")
			outdir := filepath.Join(root, "out")
			err := os.MkdirAll(srcdir, 0o755)
			a.NoError(err)
			err = os.MkdirAll(outdir, 0o755)
			a.NoError(err)
			err = os.MkdirAll(filepath.Join(outdir, "dump"), 0o755)
			a.NoError(err)

			makeFS(t, t0, root, tt.fs)

			cache, err := newCache(filepath.Join(temp, "cache"))
			a.NoError(err)
			t.Cleanup(func() { _ = cache.Close() })

			prefillCache(t, root, cache, tt.cache)

			cfg := &config{
				SourceDirectories: []string{srcdir},
				OutputDirectory:   outdir,
				DumpSubdirectory:  "dump",
				Command:           cmd,
				Extensions:        []string{".yes"},
			}

			err = process(t.Context(), cfg, cache)
			a.NoError(err, "unexpected process error")

			assertFS(t, tt.expectFS, root)

			assertCache(t, root, t0, t1, tt.expectCache, cache)
		})
	}
}

func hsh(tb testing.TB, s string) hash {
	tb.Helper()

	if strings.ToLower(s) == "wrong" {
		return hsh(tb, "00000000000000000000000000000000")
	}

	require.Len(tb, s, 32)

	var out hash

	b, err := hex.DecodeString(s)
	require.NoError(tb, err)

	copy(out[:], b)
	return out
}

func makeFS(tb testing.TB, mtime time.Time, root string, fs map[string]string) {
	tb.Helper()
	a := require.New(tb)

	for path, content := range fs {
		path := filepath.Join(root, path)

		err := os.MkdirAll(filepath.Dir(path), 0o755)
		a.NoError(err)

		f, err := os.Create(path)
		a.NoError(err)
		defer func() { _ = f.Close() }()

		_, err = f.WriteString(content)
		a.NoError(err)

		err = os.Chtimes(path, mtime, mtime)
		a.NoError(err)
	}
}

// assertFS asserts that the filesystem at actualRoot contains exactly the same files as the
// expectFS map.
func assertFS(tb testing.TB, expectFS map[string]string, actualRoot string) {
	tb.Helper()
	a := require.New(tb)

	root := strings.TrimSuffix(actualRoot, "/") + "/"

	actualFS := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err // Stop iteration.
		}

		if d.IsDir() {
			return nil // Skip directories.
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		path = strings.TrimPrefix(path, root)
		actualFS[path] = string(content)

		return nil
	})
	a.NoError(err, "iteration error")

	if expectFS == nil {
		a.Len(actualFS, 0, "filesystem should be empty")
		return
	}
	a.EqualValues(expectFS, actualFS, "filesystem mismatch")
}

func prefillCache(tb testing.TB, root string, cache *cache, elements []any) {
	tb.Helper()
	a := require.New(tb)

	for _, el := range elements {
		switch entry := el.(type) {
		case *sourceFile:
			entry.Path = filepath.Join(root, entry.Path)
			err := cache.InsertSourceFile(tb.Context(), entry)
			a.NoError(err)
		case *outputFile:
			entry.Path = filepath.Join(root, entry.Path)
			err := cache.InsertOutputFile(tb.Context(), entry)
			a.NoError(err)
		}
	}
}

// assertCache asserts that the actual cache exactly matches expectedElements.
func assertCache(tb testing.TB, root string, t0, t1 time.Time, expectedElements []any, actualCache *cache) {
	tb.Helper()
	a := require.New(tb)

	root = strings.TrimSuffix(root, "/") + "/"

	actSrc, err := actualCache.listSourceFiles(tb.Context())
	a.NoError(err)
	actOut, err := actualCache.listOutputFiles(tb.Context())
	a.NoError(err)

	actual := make([]any, 0)
	for _, el := range actSrc {
		el.Path = strings.TrimPrefix(el.Path, root)
		if !el.Mtime.Equal(t0) {
			el.Mtime = t1
		}
		actual = append(actual, el)
	}
	for _, el := range actOut {
		el.Path = strings.TrimPrefix(el.Path, root)
		if !el.Mtime.Equal(t0) {
			el.Mtime = t1
		}
		actual = append(actual, el)
	}

	if expectedElements == nil {
		a.Len(actual, 0, "cache should be empty")
		return
	}
	a.ElementsMatch(expectedElements, actual, "cache mismatch")
}

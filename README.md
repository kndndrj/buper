# BUPER!?

A very specific tool I need for my sync/backup jobs.

**Here's how it works:**

- Look into `source` directories for new files.
- Apply command on these new files and move them to `output/dump` directory of the outputs.
- Look through the files in `dump` subdirectory and delete them if any file with the same contents
  is found in the `output` directory.
- Use cache for all of the above.

Example flags:

```
--source-dirs src1,src2           # List of source directories (scanned recursively).
--command 'mv $in $out'           # Must be in single quotes.
--output-dir out                  # This directory will be recursively searched for already processed files.
--dump-subdir bump                # This will put newly processed files in `out/bump/`
--exclude '**/*.png,**/.git/**'   # Paths to exclude.
```

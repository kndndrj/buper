-- +goose Up
CREATE TABLE IF NOT EXISTS source_files (
    -- These are used to check cache validity.
    path TEXT PRIMARY KEY,
    size INTEGER NOT NULL,           -- Bytes.
    mtime INTEGER NOT NULL,          -- Unix nanoseconds.
    processed_command TEXT NOT NULL, -- Command applied to the source file to produce the output.

    -- These are cached values.
    processed_hash BLOB NOT NULL,    -- md5 checksum of the output file produced by applying the command.

    valid INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS output_files (
    -- These are used to check cache validity.
    path TEXT PRIMARY KEY,
    size INTEGER NOT NULL,           -- Bytes.
    mtime INTEGER NOT NULL,          -- Unix nanoseconds.

    -- These are cached values.
    hash BLOB NOT NULL,              -- md5 checksum.

    valid INTEGER NOT NULL
);

CREATE INDEX idx_output_files_hash ON output_files(hash);

-- +goose Down
DROP TABLE IF EXISTS source_files;
DROP TABLE IF EXISTS output_files;

package main

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"
)

var errLocked = errors.New("cache is locked")

type cache struct {
	tx *sql.Tx

	// Statements.
	sourceValidateAndGetHash *sql.Stmt
	sourceInsert             *sql.Stmt
	outputValidate           *sql.Stmt
	outputInsert             *sql.Stmt
	outputCheckHash          *sql.Stmt

	// Auxiliaries.
	closeConn       func() error
	closeStatements []func() error
}

func newCache(path string) (_ *cache, err error) { // WARN: Named return needed!
	dsn := &url.URL{
		Scheme: "file",
		Path:   path,
	}
	q := dsn.Query()
	q.Set("_txlock", "immediate")       // Immediately start a transaction on .BeginTx.
	q.Set("_pragma", "busy_timeout(0)") // Don't wait for acquiring the transaction.
	dsn.RawQuery = q.Encode()

	conn, err := sql.Open("sqlite", dsn.String())
	if err != nil {
		return nil, fmt.Errorf("failed opening sqlite connection: %w", err)
	}
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()

	c := &cache{
		closeConn: conn.Close,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.runMigrations(ctx, conn); err != nil {
		return nil, fmt.Errorf("failed running migrations: %w", err)
	}

	// We begin a transaction for the duration of the process.
	// We don't need transaction guarantees, but only want the database to be locked for writing by
	// other processes.
	tx, err := conn.Begin() //nolint:noctx
	if err != nil {
		return nil, errLocked
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	c.tx = tx

	// Prepared statements.

	defer func() {
		if err != nil {
			for _, fn := range c.closeStatements {
				_ = fn()
			}
		}
	}()

	sourceValidateAndGetHash, err := tx.PrepareContext(ctx, `
		UPDATE source_files
		SET valid = 1
		WHERE
			path = ? AND
			size = ? AND
			mtime = ? AND
			processed_command = ?
		RETURNING processed_hash
	`)
	if err != nil {
		return nil, fmt.Errorf("failed preparing validate source statement: %w", err)
	}
	c.closeStatements = append(c.closeStatements, sourceValidateAndGetHash.Close)
	c.sourceValidateAndGetHash = sourceValidateAndGetHash

	sourceInsert, err := tx.PrepareContext(ctx, `
		INSERT INTO source_files (path, size, mtime, processed_command, processed_hash, valid)
		VALUES (?, ?, ?, ?, ?, 1)
		ON CONFLICT(path) DO UPDATE
		SET
			size = excluded.size,
			mtime = excluded.mtime,
			processed_command = excluded.processed_command,
			processed_hash = excluded.processed_hash,
			valid = 1
		;
	`)
	if err != nil {
		return nil, fmt.Errorf("failed preparing insert source statement: %w", err)
	}
	c.closeStatements = append(c.closeStatements, sourceInsert.Close)
	c.sourceInsert = sourceInsert

	outputvalidate, err := tx.PrepareContext(ctx, `
		UPDATE output_files
		SET valid = 1
		WHERE
			path = ? AND
			size = ? AND
			mtime = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed preparing validate output statement: %w", err)
	}
	c.closeStatements = append(c.closeStatements, outputvalidate.Close)
	c.outputValidate = outputvalidate

	outputInsert, err := tx.PrepareContext(ctx, `
		INSERT INTO output_files (path, size, mtime, hash, valid)
		VALUES (?, ?, ?, ?, 1)
		ON CONFLICT(path) DO UPDATE
		SET
			size = excluded.size,
			mtime = excluded.mtime,
			hash = excluded.hash,
			valid = 1
		;
	`)
	if err != nil {
		return nil, fmt.Errorf("failed preparing insert output statement: %w", err)
	}
	c.closeStatements = append(c.closeStatements, outputInsert.Close)
	c.outputInsert = outputInsert

	outputCheckHash, err := tx.PrepareContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM output_files
			WHERE hash = ? AND valid = 1
		);
	`)
	if err != nil {
		return nil, fmt.Errorf("failed preparing check hash statement: %w", err)
	}
	c.closeStatements = append(c.closeStatements, outputCheckHash.Close)
	c.outputCheckHash = outputCheckHash

	return c, nil
}

//go:embed migrations/*.sql
var embededMigrations embed.FS

func (c *cache) runMigrations(ctx context.Context, conn *sql.DB) error {
	goose.SetBaseFS(embededMigrations) // Embed migrations into binary.

	if err := goose.SetDialect("sqlite"); err != nil {
		return fmt.Errorf("failed settings goose dialect: %w", err)
	}

	if err := goose.UpContext(ctx, conn, "migrations"); err != nil {
		return fmt.Errorf("failed running goose migrations: %w", err)
	}

	return nil
}

func (c *cache) Close() error {
	errs := make([]error, 0)
	if err := c.tx.Commit(); err != nil {
		errs = append(errs, fmt.Errorf("failed committing transaction: %w", err))
	}

	for _, cl := range c.closeStatements {
		if err := cl(); err != nil {
			errs = append(errs, fmt.Errorf("closing statement: %w", err))
		}
	}

	if err := c.closeConn(); err != nil {
		errs = append(errs, fmt.Errorf("closing connection: %w", err))
	}

	return errors.Join(errs...)
}

// Invalidate invalidates the whole cache.
func (c *cache) Invalidate(ctx context.Context) error {
	_, err := c.tx.ExecContext(ctx, "UPDATE source_files SET valid = 0")
	if err != nil {
		return fmt.Errorf("failed invalidating source file rows: %w", err)
	}

	_, err = c.tx.ExecContext(ctx, "UPDATE output_files SET valid = 0")
	if err != nil {
		return fmt.Errorf("failed invalidating output file rows: %w", err)
	}

	return nil
}

// CheckProcessedHashAndValidateSourceFile does these things:
// - Check if the source file record is valid and update it if it is.
// - Get the processed hash of the output derived from the source if the record is valid.
func (c *cache) CheckProcessedHashAndValidateSourceFile(ctx context.Context, path string, size int64, mtime time.Time, command string) (processedHash hash, valid bool, err error) {
	err = c.sourceValidateAndGetHash.QueryRowContext(ctx, path, size, mtime.UnixNano(), command).Scan(&processedHash)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return hash{}, false, nil // No valid record found.
		}
		return hash{}, false, fmt.Errorf("failed scanning row: %w", err)
	}

	// Valid.
	return processedHash, true, nil
}

// CheckAndValidateOutputFile checks if the cache record of the output file is valid and updates it if it is.
// Responds true if the record is valid.
func (c *cache) CheckAndValidateOutputFile(ctx context.Context, path string, size int64, mtime time.Time) (valid bool, err error) {
	res, err := c.outputValidate.ExecContext(ctx, path, size, mtime.UnixNano())
	if err != nil {
		return false, fmt.Errorf("failed updating rows: %w", err)
	}

	affrows, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed retrieving affected rows: %w", err)
	}
	if affrows > 1 {
		// Unreachable - cannot happen because of primary key on path.
		panic("too many affected rows")
	}

	// If a row was updated, the record is valid.
	return affrows == 1, nil
}

// CheckOutputFileHashExists returns true if a valid output file with the specified hash is found in
// cache.
func (c *cache) CheckOutputFileHashExists(ctx context.Context, hash hash) (exists bool, err error) {
	err = c.outputCheckHash.QueryRowContext(ctx, hash).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed scanning rows: %w", err)
	}

	return exists, nil
}

// CleanInvalid deletes invalid records.
func (c *cache) CleanInvalid(ctx context.Context) error {
	_, err := c.tx.ExecContext(ctx, "DELETE FROM source_files WHERE valid = 0")
	if err != nil {
		return fmt.Errorf("failed cleaning invalid source rows: %w", err)
	}

	_, err = c.tx.ExecContext(ctx, "DELETE FROM output_files WHERE valid = 0")
	if err != nil {
		return fmt.Errorf("failed cleaning invalid output rows: %w", err)
	}

	return nil
}

// InsertSourceFile inserts a new source file into cache.
func (c *cache) InsertSourceFile(ctx context.Context, f *sourceFile) error {
	_, err := c.sourceInsert.ExecContext(ctx, f.Path, f.Size, f.Mtime.UnixNano(), f.ProcessedCommand, f.ProcessedHash)
	if err != nil {
		return fmt.Errorf("failed inserting row: %w", err)
	}

	return nil
}

// InsertOutputFile inserts a new output file into cache.
func (c *cache) InsertOutputFile(ctx context.Context, f *outputFile) error {
	_, err := c.outputInsert.ExecContext(ctx, f.Path, f.Size, f.Mtime.UnixNano(), f.Hash)
	if err != nil {
		return fmt.Errorf("failed inserting row: %w", err)
	}

	return nil
}

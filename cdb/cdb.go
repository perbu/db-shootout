package cdbdb

import (
	"fmt"
	"os"

	"github.com/colinmarc/cdb"
	"github.com/perbu/db-shootout/keyset"
)

// CDBDB implements the BenchmarkDB interface using github.com/colinmarc/cdb
type CDBDB struct {
	filename string
	dirsize  int
	current  int
	db       *cdb.CDB // read-only handle after freezing
}

// New creates a new CDBDB instance with the given filename and directory size.
func New(filename string, dirsize int) *CDBDB {
	return &CDBDB{
		filename: filename,
		dirsize:  dirsize,
	}
}

// OpenReadOnly opens an existing CDB file for read operations.
func (b *CDBDB) OpenReadOnly() error {
	db, err := cdb.Open(b.filename)
	if err != nil {
		return fmt.Errorf("open cdb: %w", err)
	}
	b.db = db
	return nil
}

// CreateFolder creates (or overwrites) the CDB file, populates it, then freezes it.
func (b *CDBDB) CreateFolder() error {
	writer, err := cdb.Create(b.filename)
	if err != nil {
		return fmt.Errorf("create cdb: %w", err)
	}

	if err := b.populateWithWriter(writer); err != nil {
		writer.Close()
		return fmt.Errorf("populate: %w", err)
	}

	// Freeze the database (finalize the file) and close the handle.
	// Freeze returns a *cdb.CDB for reading, but we can close it immediately
	// since CreateFolder only needs to build the file, not keep it open.
	db, err := writer.Freeze()
	if err != nil {
		return fmt.Errorf("freeze: %w", err)
	}
	_ = db.Close()

	return nil
}

// Delete removes the underlying CDB file from the filesystem.
func (b *CDBDB) Delete() error {
	return os.Remove(b.filename)
}

// Close closes the read-only database handle, if open.
func (b *CDBDB) Close() error {
	if b.db != nil {
		err := b.db.Close()
		b.db = nil
		return err
	}
	return nil
}

// Populate creates a fresh CDB file and writes dirsize entries to it.
// This is separate so it can be called alone, but is also used by CreateFolder.
func (b *CDBDB) Populate() error {
	writer, err := cdb.Create(b.filename)
	if err != nil {
		return fmt.Errorf("create cdb: %w", err)
	}

	if err := b.populateWithWriter(writer); err != nil {
		writer.Close()
		return fmt.Errorf("populate: %w", err)
	}

	// Freeze and close.
	db, err := writer.Freeze()
	if err != nil {
		return fmt.Errorf("freeze: %w", err)
	}
	_ = db.Close()

	return nil
}

// populateWithWriter writes the generated key/value pairs to the given cdb.Writer.
func (b *CDBDB) populateWithWriter(writer *cdb.Writer) error {
	for i := 0; i < b.dirsize; i++ {
		key := []byte(keyset.GenerateKey(i))
		val := []byte(keyset.GenerateRandomContent(64))
		if err := writer.Put(key, val); err != nil {
			return fmt.Errorf("put: %w", err)
		}
	}
	// reset current index after populating
	b.current = 0
	return nil
}

// Next returns the next key in sequence, mimicking the same approach as the SQLite example.
func (b *CDBDB) Next() (string, bool, error) {
	if b.current >= b.dirsize {
		return "", false, nil
	}
	entry := keyset.GenerateKey(b.current)
	b.current++
	return entry, true, nil
}

// LookupValid retrieves content for the generated key at the given index.
func (b *CDBDB) LookupValid(index int) (string, error) {
	if b.db == nil {
		return "", fmt.Errorf("database is not open")
	}
	if index < 0 || index >= b.dirsize {
		return "", fmt.Errorf("index out of bounds")
	}
	filename := keyset.GenerateKey(index)
	val, err := b.db.Get([]byte(filename))
	if err != nil {
		return "", fmt.Errorf("get: %w", err)
	}
	if val == nil {
		return "", fmt.Errorf("no row found")
	}
	return string(val), nil
}

// LookupInvalid attempts to retrieve a key we know does not exist.
func (b *CDBDB) LookupInvalid() (string, error) {
	if b.db == nil {
		return "", fmt.Errorf("database is not open")
	}
	val, err := b.db.Get([]byte("invalid"))
	if err != nil {
		return "", fmt.Errorf("get: %w", err)
	}
	if val != nil {
		return "", fmt.Errorf("row found")
	}
	return "", nil
}

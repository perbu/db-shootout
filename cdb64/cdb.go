package cdbdb64

import (
	"fmt"
	"os"

	"github.com/perbu/cdb"
	"github.com/perbu/db-shootout/keyset"
)

// CDBDB implements the BenchmarkDB interface using github.com/colinmarc/cdbdb
type CDBDB struct {
	filename string
	dirsize  int
	current  int
	db       *cdb.MmapCDB
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
	db, err := cdb.OpenMmap(b.filename)
	if err != nil {
		return fmt.Errorf("open cdbdb: %w", err)
	}
	b.db = db
	return nil
}

// CreateFolder creates (or overwrites) the CDB file, populates it, then freezes it.
func (b *CDBDB) CreateFolder() error {
	writer, err := cdb.Create(b.filename)
	if err != nil {
		return fmt.Errorf("create cdbdb: %w", err)
	}

	if err := b.populateWithWriter(writer); err != nil {
		writer.Close()
		return fmt.Errorf("populate: %w", err)
	}

	// Freeze the database (finalize the file) and close the handle.
	// Freeze returns a *cdbdb.CDB for reading, but we can close it immediately
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
		return fmt.Errorf("create cdbdb: %w", err)
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

// populateWithWriter writes the generated key/value pairs to the given cdbdb.Writer.
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

// Next returns the next key in sequence and actually reads the data from the database.
func (b *CDBDB) Next() (string, bool, error) {
	if b.current >= b.dirsize {
		return "", false, nil
	}
	if b.db == nil {
		return "", false, fmt.Errorf("database is not open")
	}

	// Generate the key for this index
	key := keyset.GenerateKey(b.current)

	// Actually read the data from the database to simulate a real readdir operation
	val, err := b.db.Get([]byte(key))
	if err != nil {
		return "", false, fmt.Errorf("get key %s: %w", key, err)
	}
	if val == nil {
		return "", false, fmt.Errorf("key %s not found", key)
	}

	b.current++
	return key, true, nil
}

// LookupValid retrieves content for the generated key at the given index.
func (b *CDBDB) Lookup(index int, valid bool) (string, error) {
	if b.db == nil {
		return "", fmt.Errorf("database is not open")
	}
	if index < 0 || index >= b.dirsize {
		return "", fmt.Errorf("index out of bounds")
	}
	var filename string
	switch valid {
	case true:
		filename = keyset.GenerateKey(index)
	case false:
		filename = keyset.GenerateInvalidKey(index)
	}
	val, err := b.db.Get([]byte(filename))
	if err != nil {
		return "", fmt.Errorf("get: %w", err)
	}
	if val == nil {
		return "", fmt.Errorf("no row found")
	}
	return string(val), nil
}

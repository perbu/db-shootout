package boltdb

import (
	"fmt"
	"os"

	bolt "github.com/openkvlab/boltdb"
	"github.com/perbu/db-shootout/keyset"
)

// BoltDB implements the BenchmarkDB interface using BoltDB
type BoltDB struct {
	filename string
	dirsize  int
	current  int
	db       *bolt.DB
	bucket   []byte
}

// New creates a new BoltDB instance with the given filename and directory size
func New(filename string, dirsize int) *BoltDB {
	return &BoltDB{
		filename: filename,
		dirsize:  dirsize,
		bucket:   []byte("directory"),
	}
}

// OpenReadOnly opens an existing BoltDB file for read operations
func (b *BoltDB) OpenReadOnly() error {
	db, err := bolt.Open(b.filename, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("open bolt: %w", err)
	}
	b.db = db
	return nil
}

// CreateFolder creates (or overwrites) the BoltDB file and populates it
func (b *BoltDB) CreateFolder() error {
	// Open with write permissions
	db, err := bolt.Open(b.filename, 0o600, nil)
	if err != nil {
		return fmt.Errorf("create bolt: %w", err)
	}
	b.db = db

	// Create bucket and populate in a single transaction
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(b.bucket)
		if err != nil {
			return fmt.Errorf("create bucket: %w", err)
		}

		// Populate the bucket
		for i := 0; i < b.dirsize; i++ {
			key := []byte(keyset.GenerateKey(i))
			val := []byte(keyset.GenerateRandomContent(64))
			if err := bucket.Put(key, val); err != nil {
				return fmt.Errorf("put: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		b.db.Close()
		return fmt.Errorf("populate: %w", err)
	}

	// Reset current index after populating
	b.current = 0
	return nil
}

// Delete removes the underlying BoltDB file from the filesystem
func (b *BoltDB) Delete() error {
	if b.db != nil {
		if err := b.db.Close(); err != nil {
			return fmt.Errorf("close before delete: %w", err)
		}
		b.db = nil
	}
	return os.Remove(b.filename)
}

// Close closes the database handle, if open
func (b *BoltDB) Close() error {
	if b.db != nil {
		err := b.db.Close()
		b.db = nil
		return err
	}
	return nil
}

// Next returns the next key in sequence
func (b *BoltDB) Next() (string, bool, error) {
	if b.current >= b.dirsize {
		return "", false, nil
	}
	entry := keyset.GenerateKey(b.current)
	b.current++
	return entry, true, nil
}

// Lookup retrieves content for the generated key at the given index
func (b *BoltDB) Lookup(index int, valid bool) (string, error) {
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

	var value string
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		if bucket == nil {
			return fmt.Errorf("bucket not found")
		}
		val := bucket.Get([]byte(filename))
		if val == nil {
			return fmt.Errorf("no row found")
		}
		value = string(val)
		return nil
	})
	if err != nil {
		return "", err
	}
	return value, nil
}

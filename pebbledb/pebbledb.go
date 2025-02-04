package pebbledb

import (
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/perbu/db-shootout/keyset"
)

type PebbleDB struct {
	filename string
	dirsize  int
	current  int
	db       *pebble.DB
	logger   pebble.Logger
}

func New(filename string, dirsize int, b *testing.B) *PebbleDB {
	return &PebbleDB{
		filename: filename,
		dirsize:  dirsize,
		logger:   &testLogger{b: b},
	}
}

func (p *PebbleDB) OpenReadOnly() error {
	opts := &pebble.Options{ReadOnly: true}
	opts.Logger = p.logger
	db, err := pebble.Open(p.filename, opts)
	if err != nil {
		return fmt.Errorf("open pebble: %w", err)
	}
	p.db = db
	return nil
}

func (p *PebbleDB) CreateFolder() error {
	opts := &pebble.Options{}
	opts.Logger = p.logger
	db, err := pebble.Open(p.filename, opts)
	if err != nil {
		return fmt.Errorf("create pebble: %w", err)
	}
	p.db = db

	batch := p.db.NewBatch()
	defer batch.Close()

	for i := 0; i < p.dirsize; i++ {
		key := []byte(keyset.GenerateKey(i))
		val := []byte(keyset.GenerateRandomContent(64))
		if err := batch.Set(key, val, pebble.Sync); err != nil {
			return fmt.Errorf("set: %w", err)
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	p.current = 0
	return nil
}

func (p *PebbleDB) Delete() error {
	if p.db != nil {
		if err := p.db.Close(); err != nil {
			return fmt.Errorf("close before delete: %w", err)
		}
		p.db = nil
	}
	return os.RemoveAll(p.filename)
}

func (p *PebbleDB) Close() error {
	if p.db != nil {
		err := p.db.Close()
		p.db = nil
		return err
	}
	return nil
}

func (p *PebbleDB) Next() (string, bool, error) {
	if p.current >= p.dirsize {
		return "", false, nil
	}
	entry := keyset.GenerateKey(p.current)
	p.current++
	return entry, true, nil
}

func (p *PebbleDB) Lookup(index int, valid bool) (string, error) {
	if p.db == nil {
		return "", fmt.Errorf("database is not open")
	}
	if index < 0 || index >= p.dirsize {
		return "", fmt.Errorf("index out of bounds")
	}

	var filename string
	switch valid {
	case true:
		filename = keyset.GenerateKey(index)
	case false:
		filename = keyset.GenerateInvalidKey(index)
	}

	value, closer, err := p.db.Get([]byte(filename))
	if err == pebble.ErrNotFound {
		return "", fmt.Errorf("no row found")
	}
	if err != nil {
		return "", err
	}
	defer closer.Close()

	return string(value), nil
}

// testLogger wraps testing.T and implements the pebble.Logger interface
type testLogger struct {
	b *testing.B
}

// Infof logs to testing.T
func (l *testLogger) Infof(format string, args ...interface{}) {
	// Make it quiet for regular logging
	// l.b.Logf(format, args...)
}

// Errorf logs to testing.T
func (l *testLogger) Errorf(format string, args ...interface{}) {
	// l.b.Logf(format, args...)
}

// Fatalf logs to testing.T and fails the test
func (l *testLogger) Fatalf(format string, args ...interface{}) {
	l.b.Fatalf(format, args...)
}

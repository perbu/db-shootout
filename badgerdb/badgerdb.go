package badgerdb

import (
	"fmt"
	"os"

	"github.com/dgraph-io/badger/v4"
	"github.com/perbu/db-shootout/keyset"
)

type BadgerDB struct {
	filename string
	dirsize  int
	current  int
	db       *badger.DB
}

func New(filename string, dirsize int) *BadgerDB {
	return &BadgerDB{
		filename: filename,
		dirsize:  dirsize,
	}
}

func (b *BadgerDB) OpenReadOnly() error {
	opts := badger.DefaultOptions(b.filename).WithReadOnly(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return fmt.Errorf("open badger: %w", err)
	}
	b.db = db
	return nil
}

func (b *BadgerDB) CreateFolder() error {
	opts := badger.DefaultOptions(b.filename)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return fmt.Errorf("create badger: %w", err)
	}
	b.db = db

	wb := db.NewWriteBatch()
	defer wb.Cancel()

	for i := 0; i < b.dirsize; i++ {
		key := []byte(keyset.GenerateKey(i))
		val := []byte(keyset.GenerateRandomContent(64))
		if err := wb.Set(key, val); err != nil {
			return fmt.Errorf("set: %w", err)
		}
	}

	if err := wb.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	b.current = 0
	return nil
}

func (b *BadgerDB) Delete() error {
	if b.db != nil {
		if err := b.db.Close(); err != nil {
			return fmt.Errorf("close before delete: %w", err)
		}
		b.db = nil
	}
	return os.RemoveAll(b.filename)
}

func (b *BadgerDB) Close() error {
	if b.db != nil {
		err := b.db.Close()
		b.db = nil
		return err
	}
	return nil
}

func (b *BadgerDB) Next() (string, bool, error) {
	if b.current >= b.dirsize {
		return "", false, nil
	}
	entry := keyset.GenerateKey(b.current)
	b.current++
	return entry, true, nil
}

func (b *BadgerDB) Lookup(index int, valid bool) (string, error) {
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
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(filename))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			value = string(val)
			return nil
		})
	})
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return "", fmt.Errorf("no row found")
		}
		return "", err
	}
	return value, nil
}

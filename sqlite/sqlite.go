package sqlite

import (
	"fmt"
	"github.com/perbu/db-shootout/keyset"
	"os"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

type SQLiteDB struct {
	db         *sqlite.Conn
	current    int
	dirsize    int
	selectStmt *sqlite.Stmt
	filename   string
}

func New(filename string, dirsize int) *SQLiteDB {
	return &SQLiteDB{
		filename: filename,
		dirsize:  dirsize,
	}
}

// OpenReadOnly prepares the database for operations.
func (b *SQLiteDB) OpenReadOnly() error {
	var err error
	b.db, err = sqlite.OpenConn(b.filename, sqlite.OpenReadOnly)
	if err != nil {
		return err
	}
	b.selectStmt, err = b.db.Prepare("SELECT content FROM folder WHERE key = ?")
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	return nil
}

// CreateFolder creates a virtual folder database with the given number of entries.
func (b *SQLiteDB) CreateFolder() error {
	var err error
	b.db, err = sqlite.OpenConn(b.filename, sqlite.OpenCreate|sqlite.OpenReadWrite)
	if err != nil {
		return err
	}
	if err := sqlitex.Execute(b.db, "CREATE TABLE IF NOT EXISTS folder (key TEXT, content TEXT)", nil); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	if err := b.Populate(); err != nil {
		return fmt.Errorf("populate: %w", err)
	}
	// create an index on the key column for faster lookups
	if err := sqlitex.Execute(b.db, "CREATE INDEX IF NOT EXISTS folder_key ON folder (key)", nil); err != nil {
		return fmt.Errorf("create index: %w", err)
	}
	// done. close the database
	if err := b.db.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}

func (b *SQLiteDB) Delete() error {
	return os.Remove(b.filename)
}

// Close closes the database connection.
func (b *SQLiteDB) Close() error {
	if b.selectStmt != nil {
		_ = b.selectStmt.Finalize()
	}
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

// Populate generates n random entries resembling filenames and 64-byte random content.
func (b *SQLiteDB) Populate() error {
	insert := b.db.Prep("INSERT INTO folder (key, content) VALUES (?, ?)")
	defer insert.Finalize()
	for i := 0; i < b.dirsize; i++ {
		_ = insert.Reset()
		_ = insert.ClearBindings()
		insert.BindText(1, keyset.GenerateKey(i))
		insert.BindText(2, keyset.GenerateRandomContent(64))
		_, err := insert.Step()
		if err != nil {
			return fmt.Errorf("insert step: %w", err)
		}
	}
	b.current = 0
	return nil
}

// Next iterates over all the entries in order. Used by ReadDir()
func (b *SQLiteDB) Next() (string, bool, error) {
	if b.current >= b.dirsize {
		return "", false, nil
	}
	entry := keyset.GenerateKey(b.current)
	b.current++
	return entry, true, nil
}

// LookupValid retrieves the content of the entry at the given index.
// We use a number between 0 and dirsize to generate a key. This should always succeed.
func (b *SQLiteDB) LookupValid(index int) (string, error) {
	if index < 0 || index >= b.dirsize {
		return "", fmt.Errorf("index out of bounds")
	}
	filename := keyset.GenerateKey(index)
	err := b.selectStmt.Reset()
	if err != nil {
		return "", fmt.Errorf("reset: %w", err)
	}
	b.selectStmt.BindText(1, filename)
	hasRow, err := b.selectStmt.Step()
	if err != nil {
		return "", fmt.Errorf("step: %w", err)
	}
	if !hasRow {
		return "", fmt.Errorf("no row found")
	}
	content := b.selectStmt.ColumnText(0)
	return content, nil
}

func (b *SQLiteDB) LookupInvalid() (string, error) {
	err := b.selectStmt.Reset()
	if err != nil {
		return "", fmt.Errorf("reset: %w", err)
	}
	b.selectStmt.BindText(1, "invalid")
	hasRow, err := b.selectStmt.Step()
	if err != nil {
		return "", fmt.Errorf("step: %w", err)
	}
	if hasRow {
		return "", fmt.Errorf("row found")
	}
	return "", nil
}

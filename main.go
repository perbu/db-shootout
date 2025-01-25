package main

import (
	"context"
	"fmt"
	"github.com/perbu/db-shootout/sqlite"
	"os"
	"os/signal"
	"syscall"
)

type BenchmarkDB interface {
	OpenReadOnly() error
	CreateFolder() error
	Delete() error
	Close() error
	Populate() error
	Next() (string, bool, error)
	LookupValid(index int) (string, error)
	LookupInvalid() (string, error)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	return nil
}

func NewSqliteDB(filename string, dirsize int) BenchmarkDB {
	s := sqlite.New(filename, dirsize)
	return s
}

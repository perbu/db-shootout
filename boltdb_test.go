package main

import (
	"math/rand"
	"testing"

	"github.com/perbu/db-shootout/boltdb"
)

func BenchmarkCreateFolderBolt(b *testing.B) {
	db := boltdb.New("test.db", dirsize)
	for i := 0; i < b.N; i++ {
		if err := db.CreateFolder(); err != nil {
			b.Fatalf("create folder: %v", err)
		}
		if err := db.Delete(); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

func BenchmarkLookupBolt(b *testing.B) {
	db := boltdb.New("test.db", dirsize)
	if err := db.CreateFolder(); err != nil {
		b.Fatalf("create folder: %v", err)
	}

	if err := db.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}

	if err := db.OpenReadOnly(); err != nil {
		b.Fatalf("open readonly: %v", err)
	}
	defer db.Close()
	defer db.Delete()
	// reset the benchmark timer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Lookup(rand.Intn(dirsize), true); err != nil {
			b.Fatalf("lookup valid: %v", err)
		}
	}
	// stop the benchmark timer so we don't measure the defers
	b.StopTimer()
}

func BenchmarkReaddirBolt(b *testing.B) {
	db := boltdb.New("test.db", dirsize)
	if err := db.CreateFolder(); err != nil {
		b.Fatalf("create folder: %v", err)
	}

	if err := db.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}

	// reset the benchmark timer
	b.ResetTimer()
	// a readdir is a complete reading of the directory
	for i := 0; i < b.N; i++ {
		if err := db.OpenReadOnly(); err != nil {
			b.Fatalf("open readonly: %v", err)
		}
		for entry := 0; entry < dirsize; entry++ {
			if _, _, err := db.Next(); err != nil {
				b.Fatalf("next: %v", err)
			}
		}
		if err := db.Close(); err != nil {
			b.Fatalf("close: %v", err)
		}
	}
	// stop the benchmark timer so we don't measure the cleanup
	b.StopTimer()
	if err := db.Delete(); err != nil {
		b.Fatalf("delete: %v", err)
	}
}

package main

import (
	"math/rand"
	"testing"

	"github.com/perbu/db-shootout/pebbledb"
)

func BenchmarkCreateFolderPebble(b *testing.B) {
	db := pebbledb.New("test.pebble", dirsize, b)
	for i := 0; i < b.N; i++ {
		if err := db.CreateFolder(); err != nil {
			b.Fatalf("create folder: %v", err)
		}
		if err := db.Delete(); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

func BenchmarkLookupPebble(b *testing.B) {
	db := pebbledb.New("test.pebble", dirsize, b)
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Lookup(rand.Intn(dirsize), true); err != nil {
			b.Fatalf("lookup valid: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkReaddirPebble(b *testing.B) {
	db := pebbledb.New("test.pebble", dirsize, b)
	if err := db.CreateFolder(); err != nil {
		b.Fatalf("create folder: %v", err)
	}

	if err := db.Close(); err != nil {
		b.Fatalf("close: %v", err)
	}

	b.ResetTimer()
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
	b.StopTimer()

	if err := db.Delete(); err != nil {
		b.Fatalf("delete: %v", err)
	}
}

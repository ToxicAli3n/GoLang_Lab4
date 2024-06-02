package datastore

import (
	"bufio"
	"bytes"
	_ "os"
	_ "path/filepath"
	"testing"
)

func TestDatabase_PutGet(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDb(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	key := "testKey"
	value := "testValue"
	err = db.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	retrievedValue, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if retrievedValue != value {
		t.Errorf("Expected value %s, got %s", value, retrievedValue)
	}
}

func TestDatabase_Recovery(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDb(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	key := "testKey"
	value := "testValue"
	err = db.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	db, err = NewDb(dir)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()

	retrievedValue, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get value after recovery: %v", err)
	}

	if retrievedValue != value {
		t.Errorf("Expected value %s, got %s", value, retrievedValue)
	}
}

func TestReadValue(t *testing.T) {
	record := Entry{
		Key:   "testKey",
		Value: "testValue",
	}
	data, err := record.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize record: %v", err)
	}

	reader := bufio.NewReader(bytes.NewReader(data))
	retrievedValue, err := retrieveValue(reader)
	if err != nil {
		t.Fatalf("Failed to read value: %v", err)
	}

	if retrievedValue != record.Value {
		t.Errorf("Expected value %s, got %s", record.Value, retrievedValue)
	}
}

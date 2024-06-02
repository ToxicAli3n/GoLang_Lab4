package datastore

import (
	"bufio"
	"bytes"
	"testing"
)

func TestDataRecord_Serialization(t *testing.T) {
	record := Entry{
		Key:   "testKey",
		Value: "testValue",
	}
	data := record.Serialize()

	expectedSize := 12 + len(record.Key) + len(record.Value)
	if len(data) != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, len(data))
	}
}

func TestDataRecord_RetrieveValue(t *testing.T) {
	record := Entry{
		Key:   "testKey",
		Value: "testValue",
	}
	data := record.Serialize()

	reader := bufio.NewReader(bytes.NewReader(data))
	retrievedValue, err := retrieveValue(reader)
	if err != nil {
		t.Fatalf("Failed to read value: %v", err)
	}

	if retrievedValue != record.Value {
		t.Errorf("Expected value %s, got %s", record.Value, retrievedValue)
	}
}

package datastore

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func setupTempDir(t *testing.T) string {
	t.Helper()
	dir, err := ioutil.TempDir("", "testDir")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func setupDb(t *testing.T, dir string, segmentSize int64) *Db {
	t.Helper()
	db, err := NewDb(dir, segmentSize)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestDb_Put(t *testing.T) {
	saveDirectory := setupTempDir(t)
	defer os.RemoveAll(saveDirectory)

	dataBase := setupDb(t, saveDirectory, 45)
	defer dataBase.Close()

	pairs := [][]string{
		{"1", "v1"},
		{"2", "v2"},
		{"3", "v3"},
	}
	finalPath := filepath.Join(saveDirectory, outFileName+"0")
	outFile, err := os.Open(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer outFile.Close()

	t.Run("check put and get methods", func(t *testing.T) {
		for _, pair := range pairs {
			err := dataBase.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Unable to put %s: %v", pair[0], err)
			}
			actual, err := dataBase.Get(pair[0])
			if err != nil {
				t.Errorf("Unable to get %s: %v", pair[0], err)
			}
			if actual != pair[1] {
				t.Errorf("Invalid value returned. Expected: %s, Actual: %s", pair[1], actual)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	expectedStateSize := outInfo.Size()

	t.Run("check file size remains the same after re-putting same data", func(t *testing.T) {
		for _, pair := range pairs {
			err := dataBase.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Unable to put %s: %v", pair[0], err)
			}
		}

		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		actualStateSize := outInfo.Size()
		if expectedStateSize != actualStateSize {
			t.Errorf("Size mismatch: Expected: %d, Actual: %d", expectedStateSize, actualStateSize)
		}
	})

	t.Run("check data persistence after DB reopening", func(t *testing.T) {
		if err := dataBase.Close(); err != nil {
			t.Fatal(err)
		}
		dataBase = setupDb(t, saveDirectory, 45)

		for _, pair := range pairs {
			actual, err := dataBase.Get(pair[0])
			if err != nil {
				t.Errorf("Unable to get %s: %v", pair[1], err)
			}
			expected := pair[1]
			if actual != expected {
				t.Errorf("Invalid value returned. Expected: %s, Actual: %s", expected, actual)
			}
		}
	})
}

func TestDb_Segmentation(t *testing.T) {
	saveDirectory, err := ioutil.TempDir("", "testDir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(saveDirectory)

	db, err := NewDb(saveDirectory, 35)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("check creation of new file", func(t *testing.T) {
		db.Put("1", "v1")
		db.Put("2", "v2")
		db.Put("3", "v3")
		db.Put("2", "v5")
		actualTwoFiles := len(db.segments)
		expected2Files := 2
		if actualTwoFiles != expected2Files {
			t.Errorf("An error occurred during segmentation. Expected 2 files, but received %d.", len(db.segments))
		}
	})

	t.Run("check starting segmentation", func(t *testing.T) {
		db.Put("4", "v4")
		actualTreeFiles := len(db.segments)
		expected3Files := 3
		if actualTreeFiles != expected3Files {
			t.Errorf("An error occurred during segmentation. Expected 3 files, but received %d.", len(db.segments))
		}

		time.Sleep(2 * time.Second)

		actualTwoFiles := len(db.segments)
		expected2Files := 2
		if actualTwoFiles != expected2Files {
			t.Errorf("An error occurred during segmentation. Expected 2 files, but received %d.", len(db.segments))
		}
	})

	t.Run("check not storing new values of duplicate keys", func(t *testing.T) {
		actual, _ := db.Get("2")
		expected := "v5"
		if actual != expected {
			t.Errorf("An error occurred during segmentation. Expected: %s, Actual: %s", expected, actual)
		}
	})

	t.Run("check szie", func(t *testing.T) {
		file, err := os.Open(db.segments[0].filePath)
		defer file.Close()

		if err != nil {
			t.Error(err)
		}
		inf, _ := file.Stat()
		actual := inf.Size()
		expected := int64(45)
		if actual != expected {
			t.Errorf("An error occurred during segmentation. Expected: %d, Actual: %d", expected, actual)
		}
	})
}

func TestDb_Delete(t *testing.T) {
	saveDirectory := setupTempDir(t)
	defer os.RemoveAll(saveDirectory)

	db := setupDb(t, saveDirectory, 150)
	defer db.Close()

	// put some initial data
	if err := db.Put("key1", "value1"); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("key2", "value2"); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("key3", "value3"); err != nil {
		t.Fatal(err)
	}

	t.Run("delete operation", func(t *testing.T) {
		db.Delete("key2")

		_, err := db.Get("key2")
		if err != ErrNotFound {
			t.Errorf("Expected ErrNotFound for deleted key, got: %v", err)
		}

		val, err := db.Get("key1")
		if err != nil {
			t.Errorf("Failed to get existing key: %v", err)
		}
		if val != "value1" {
			t.Errorf("Bad value returned. Expected: value1, got: %s", val)
		}
	})

	t.Run("delete non-existing key", func(t *testing.T) {
		db.Delete("key4")

		_, err := db.Get("key4")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Expected ErrNotFound for non-existing key, got: %v", err)
		}
	})
}

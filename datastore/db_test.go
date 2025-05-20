package datastore

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

var segSize int64 = 8192

func TestDb(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp, segSize)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	pairs := [][]string{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k2", "v2.1"},
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("not exists", func(t *testing.T) {
		if size, err := db.Size(); err == nil && size == 0 {
			t.Fatal("dababase wasnt expected to be empty")
		} else if err != nil {
			t.Fatal(err)
		}
		_, err = db.Get("does not exist")
		if err != ErrNotFound {
			t.Errorf("expected %s, got %s", ErrNotFound, err)
		}
	})

	t.Run("file growth", func(t *testing.T) {
		sizeBefore, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		sizeAfter, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		if sizeAfter <= sizeBefore {
			t.Errorf("Size does not grow after put (before %d, after %d)", sizeBefore, sizeAfter)
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = Open(tmp, segSize)
		if err != nil {
			t.Fatal(err)
		}

		uniquePairs := make(map[string]string)
		for _, pair := range pairs {
			uniquePairs[pair[0]] = pair[1]
		}

		for key, expectedValue := range uniquePairs {
			value, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
			if value != expectedValue {
				t.Errorf("Get(%q) = %q, wanted %q", key, value, expectedValue)
			}
		}
	})

	t.Run("segmentation", func(t *testing.T) {
		tempDir := t.TempDir()
		segmentSize := int64(100)
		db, err := Open(tempDir, segmentSize)
		if err != nil {
			t.Error(err)
		}
		defer db.Close()
		pairs := map[string]string{
			"key1": "value-one",
			"key2": "value-two",
			"key3": "value-three",
			"key4": "value-four",
			"key5": "value-five",
		}

		for k, v := range pairs {
			err := db.Put(k, v)
			if err != nil {
				t.Fatal(err)
			}
		}
		for k, expected := range pairs {
			val, err := db.Get(k)
			if err != nil {
				t.Errorf("got error for key %s: %s", k, err)
			}
			if val != expected {
				t.Errorf("expected %s, got %s", expected, val)
			}

			files, err := filepath.Glob(filepath.Join(tempDir, "current-data*"))
			if err != nil {
				t.Fatal(err)
			}
			if len(files) < 2 {
				t.Error("expected multiple segments")
			}
		}
		var key1, val1 = "key2", "value-six"
		err1 := db.Put(key1, val1)
		if err1 != nil {
			t.Fatal(err1)
		}
		val2, err2 := db.Get(key1)
		if err2 != nil {
			t.Fatal(err2)
		}
		if val2 != val1 {
			t.Errorf("expected %s, got %s", val1, val2)
		}
	})
}

func TestSegmentMerge(t *testing.T) {
	tempDir := t.TempDir()
	db, err := Open(tempDir, 100)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Failed to close database: %v", err)
		}
	}()

	var entries []struct {
		key, value string
	}
	for i := 1; i <= 9; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		entries = append(entries, struct {
			key, value string
		}{key: key, value: value})
	}

	for _, entry := range entries {
		err := db.Put(entry.key, entry.value)
		if err != nil {
			t.Fatalf("Failed to insert record (%s, %s): %v", entry.key, entry.value, err)
		}
	}

	select {
	case <-time.After(3 * time.Second):
		t.Error("Merge did not complete in time")
	default:
		db.mergeWg.Wait()
	}

	for _, entry := range entries {
		result, err := db.Get(entry.key)
		if err != nil {
			t.Errorf("Failed to get value for key %s: %v", entry.key, err)
		} else if result != entry.value {
			t.Errorf("Expected value %s for key %s, but got %s", entry.value, entry.key, result)
		}
	}

	db.indexMutex.RLock()
	segmentsAfterMerge := len(db.segments)
	db.indexMutex.RUnlock()

	if segmentsAfterMerge != 2 {
		t.Errorf("Expected 2 segments after merge, but got %d", segmentsAfterMerge)
	}
}

func TestDuplicateHandlingDuringMerge(t *testing.T) {
	tempDir := t.TempDir()
	db, err := Open(tempDir, 100)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Failed to close database: %v", err)
		}
	}()

	entries := []struct {
		key, value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3.53"},
		{"key2", "value2.1"},
		{"key4", "value4"},
		{"key5", "value5"},
		{"key2", "value2.1"},
		{"key6", "value6"},
		{"key3", "value3"},
	}

	for _, entry := range entries {
		err := db.Put(entry.key, entry.value)
		if err != nil {
			t.Fatalf("Failed to insert record (%s, %s): %v", entry.key, entry.value, err)
		}
	}

	select {
	case <-time.After(3 * time.Second):
		t.Error("Merge did not complete in time")
	default:
		db.mergeWg.Wait()
	}

	expected := map[string]string{
		"key1": "value1",
		"key2": "value2.1",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
		"key6": "value6",
	}

	for key, expectedVal := range expected {
		val, err := db.Get(key)
		if err != nil {
			t.Errorf("Failed to get value for key %s: %v", key, err)
		} else if val != expectedVal {
			t.Errorf("Expected value %s for key %s, but got %s", expectedVal, key, val)
		}
	}

	db.indexMutex.RLock()
	segmentsAfterMerge := len(db.segments)
	db.indexMutex.RUnlock()

	if segmentsAfterMerge != 2 {
		t.Errorf("Expected 2 segments after merge, but got %d", segmentsAfterMerge)
	}

	seg := db.segments[0]
	seg.mutex.RLock()
	defer seg.mutex.RUnlock()

	for key := range expected {
		_, exists := seg.index[key]
		if !exists {
			t.Errorf("Key %s not found in merged segment index", key)
		}
	}
}

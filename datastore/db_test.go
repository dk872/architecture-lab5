package datastore

import (
	"path/filepath"
	"testing"
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

	t.Run("not exists", func (t *testing.T) {
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

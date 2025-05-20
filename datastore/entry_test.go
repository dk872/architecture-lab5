package datastore

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"testing"
)

func TestEntry_Encode(t *testing.T) {
	e := entry{
		key:   "key",
		value: "value",
	}
	e.Decode(e.Encode())
	if e.key != "key" {
		t.Error("incorrect key")
	}
	if e.value != "value" {
		t.Error("incorrect value")
	}
}

func TestReadValue(t *testing.T) {
	var (
		a, b entry
	)
	a = entry{
		key:   "key",
		value: "value",
	}
	originalBytes := a.Encode()

	b.Decode(originalBytes)
	t.Log("encode/decode", a, b)
	if a != b {
		t.Error("Encode/Decode mismatch")
	}

	b = entry{}
	n, err := b.DecodeFromReader(bufio.NewReader(bytes.NewReader(originalBytes)))
	if err != nil {
		t.Fatal(err)
	}
	t.Log("encode/decodeFromReader", a, b)
	if a != b {
		t.Error("Encode/DecodeFromReader mismatch")
	}
	if n != len(originalBytes) {
		t.Errorf("DecodeFromReader() read %d bytes, expected %d", n, len(originalBytes))
	}
}

func TestSHA1IntegrityDuringPutGet(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp, 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for _, pair := range pairs {
		err := db.Put(pair[0], pair[1])
		if err != nil {
			t.Errorf("Failed to insert key %s: %s", pair[0], err)
		}

		value, err := db.Get(pair[0])
		if err != nil {
			t.Errorf("Failed to get key %s: %s", pair[0], err)
		}

		if value != pair[1] {
			t.Errorf("Data mismatch for key %s. Expected %s, got %s", pair[0], pair[1], value)
		}
	}
}

func TestSHA1IntegrityDuringEncodingDecoding(t *testing.T) {
	original := entry{
		key:   "alpha",
		value: "bravo",
	}
	encoded := original.Encode()

	var decoded entry
	_, err := decoded.DecodeFromReader(bufio.NewReader(bytes.NewReader(encoded)))
	if err != nil {
		t.Fatalf("DecodeFromReader failed: %v", err)
	}

	if decoded.key != original.key {
		t.Errorf("Key mismatch: expected %s, got %s", original.key, decoded.key)
	}
	if decoded.value != original.value {
		t.Errorf("Value mismatch: expected %s, got %s", original.value, decoded.value)
	}

	expectedHash := sha1.Sum([]byte(original.value))
	if decoded.hash != expectedHash {
		t.Errorf("SHA-1 hash mismatch: expected %x, got %x", expectedHash, decoded.hash)
	}
}

func TestSHA1HashMismatch(t *testing.T) {
	original := entry{
		key:   "alpha",
		value: "bravo",
	}
	encoded := original.Encode()

	var decoded entry
	_, err := decoded.DecodeFromReader(bufio.NewReader(bytes.NewReader(encoded)))
	if err != nil {
		t.Fatalf("DecodeFromReader failed: %v", err)
	}

	decoded.hash[0]++

	expectedHash := sha1.Sum([]byte(original.value))
	if decoded.hash == expectedHash {
		t.Errorf("SHA-1 hash should not match. Expected %x, got %x", expectedHash, decoded.hash)
	}
}

func TestSHA1HashChangeOnValueModification(t *testing.T) {
	original := entry{
		key:   "alpha",
		value: "bravo",
	}
	encoded := original.Encode()

	var decoded entry
	_, err := decoded.DecodeFromReader(bufio.NewReader(bytes.NewReader(encoded)))
	if err != nil {
		t.Fatalf("DecodeFromReader failed: %v", err)
	}

	original.value = "new value"

	expectedHash := sha1.Sum([]byte(original.value))
	if decoded.hash == expectedHash {
		t.Errorf("SHA-1 hash should change when the value changes. Expected %x, got %x", expectedHash, decoded.hash)
	}
}

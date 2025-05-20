package datastore

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type entry struct {
	key, value string
	hash       [20]byte
}

// 0           4    8     kl+8  kl+12	kl+vl+12     <-- offset
// (full size) (kl) (key) (vl)  (value)	(hash)
// 4           4    ....  4     .....	20		     <-- length

func (e *entry) Encode() []byte {
	kl, vl := len(e.key), len(e.value)
	hash := sha1.Sum([]byte(e.value))
	e.hash = hash

	size := kl + vl + 12 + len(hash)
	res := make([]byte, size)

	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(vl))
	copy(res[kl+12:], e.value)
	copy(res[kl+12+vl:], hash[:])

	return res
}

func (e *entry) Decode(input []byte) {
	e.key = decodeString(input[4:])

	kl := binary.LittleEndian.Uint32(input[4:])
	keyEnd := 8 + int(kl)

	e.value = decodeString(input[keyEnd:])

	vl := binary.LittleEndian.Uint32(input[keyEnd:])
	valEnd := keyEnd + 4 + int(vl)

	copy(e.hash[:], input[valEnd:])
}

func decodeString(v []byte) string {
	l := binary.LittleEndian.Uint32(v)
	buf := make([]byte, l)
	copy(buf, v[4:4+int(l)])
	return string(buf)
}

func (e *entry) DecodeFromReader(in *bufio.Reader) (int, error) {
	sizeBuf, err := in.Peek(4)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, err
		}
		return 0, fmt.Errorf("DecodeFromReader, cannot read size: %w", err)
	}
	size := int(binary.LittleEndian.Uint32(sizeBuf))
	buf := make([]byte, size)

	n, err := in.Read(buf)
	if err != nil {
		return n, fmt.Errorf("DecodeFromReader, cannot read record: %w", err)
	}

	e.Decode(buf)

	expectedHash := sha1.Sum([]byte(e.value))
	if e.hash != expectedHash {
		return n, fmt.Errorf("data integrity check failed: sha1 mismatch")
	}

	return n, nil
}

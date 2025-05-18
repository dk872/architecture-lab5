package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)


const outFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type FileSegment struct {
	index   hashIndex
	outPath string
	mutex   sync.RWMutex
}

type Db struct {
	out           *os.File
	outOffset     int64
	dir           string
	segmentSize   int64
	segmentNumber int
	segments      []*FileSegment
	indexMutex    sync.RWMutex
}

func Open(dir string, segmentSize int64) (*Db, error) {
	db := &Db{
		segments:      make([]*FileSegment, 0),
		dir:           dir,
		segmentSize:   segmentSize,
		segmentNumber: 0,
	}
	
	err := db.newSegment()
	if err != nil {
		return nil, err
	}
	
	err = db.recover()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	
	return db, nil
}

func (db *Db) newSegment() error {
	outFile := fmt.Sprintf("%s%d", outFileName, db.segmentNumber)
	outFilePath := filepath.Join(db.dir, outFile)
	db.segmentNumber++

	f, err := os.OpenFile(outFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	newFileSegment := &FileSegment{
		outPath: outFilePath,
		index:   make(hashIndex),
	}

	if db.out != nil {
		db.out.Close()
	}
	
	db.out = f
	db.outOffset = 0
	db.segments = append(db.segments, newFileSegment)

	if len(db.segments) >= 3 {
		db.mergeSegments()
	}
	
	return nil
}

func (s *FileSegment) getValue(position int64) (string, error) {
	f, err := os.Open(s.outPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	
	reader := bufio.NewReader(f)
	_, err = reader.Discard(int(position))
	if err != nil {
		return "", err
	}

	var record entry
	_, err = record.DecodeFromReader(reader)
	if err != nil {
		return "", err
	}
	
	return record.value, nil
}

func (db *Db) mergeSegments() {
	go func() {
		newOutFile := fmt.Sprintf("%s%d", outFileName, db.segmentNumber)
		newPath := filepath.Join(db.dir, newOutFile)
		db.segmentNumber++

		newSeg := &FileSegment{
			outPath: newPath,
			index:   make(hashIndex),
		}
		var offset int64

		f, err := os.OpenFile(newPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return
		}
		defer f.Close()
		
		lastInd := len(db.segments) - 2
		
		for i := 0; i <= lastInd; i++ {
			seg := db.segments[i]
			
			seg.mutex.RLock()
			for key, index := range seg.index {
				if i < lastInd {
					duplicateFlag := false
					for _, s := range db.segments[i+1:lastInd+1] {
						s.mutex.RLock()
						_, ok := s.index[key]
						s.mutex.RUnlock()
						
						if ok {
							duplicateFlag = true
							break
						}
					}
					
					if duplicateFlag {
						continue
					}
				}
				
				value, err := seg.getValue(index)
				if err != nil {
					continue
				}
				
				entry := entry{
					key:   key,
					value: value,
				}
				
				data := entry.Encode()
				n, err := f.Write(data)
				if err == nil {
					newSeg.index[key] = offset
					offset += int64(n)
				}
			}
			seg.mutex.RUnlock()
		}
		
		db.indexMutex.Lock()
		db.segments = []*FileSegment{newSeg, db.segments[len(db.segments)-1]}
		db.indexMutex.Unlock()
	}()
}

func (db *Db) recover() error {
	for i, segment := range db.segments {
		f, err := os.Open(segment.outPath)
		if err != nil {
			return err
		}
		
		if i == len(db.segments)-1 {
			info, err := f.Stat()
			if err != nil {
				f.Close()
				return err
			}
			db.outOffset = info.Size()
		}
		
		reader := bufio.NewReader(f)
		var offset int64 = 0
		
		for {
			var record entry
			n, err := record.DecodeFromReader(reader)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				f.Close()
				return err
			}
			
			segment.mutex.Lock()
			segment.index[record.key] = offset
			segment.mutex.Unlock()
			
			offset += int64(n)
		}
		
		f.Close()
	}
	
	return nil
}

func (db *Db) Close() error {
	if db.out != nil {
		return db.out.Close()
	}
	return nil
}

func (db *Db) Get(key string) (string, error) {
	db.indexMutex.RLock()
	defer db.indexMutex.RUnlock()
	
	for i := range db.segments {
		segment := db.segments[len(db.segments)-i-1]
		segment.mutex.RLock()
		
		position, ok := segment.index[key]
		segment.mutex.RUnlock()
		
		if ok {
			return segment.getValue(position)
		}
	}
	
	return "", ErrNotFound
}

func (db *Db) Put(key, value string) error {
	db.indexMutex.Lock()
	defer db.indexMutex.Unlock()
	
	e := entry{
		key:   key,
		value: value,
	}
	
	encodedEntry := e.Encode()
	entrySize := int64(len(encodedEntry))
	
	stat, err := db.out.Stat()
	if err != nil {
		return err
	}
	
	if stat.Size()+entrySize > db.segmentSize {
		err := db.newSegment()
		if err != nil {
			return err
		}
	}
	
	n, err := db.out.Write(encodedEntry)
	if err != nil {
		return err
	}
	
	currentSegment := db.segments[len(db.segments)-1]
	currentSegment.mutex.Lock()
	currentSegment.index[key] = db.outOffset
	currentSegment.mutex.Unlock()
	
	db.outOffset += int64(n)
	
	return nil
}

func (db *Db) Size() (int64, error) {
	info, err := db.out.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}
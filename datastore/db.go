package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const outFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	out       *os.File
	outPath   string
	outOffset int64

	index hashIndex
}

func NewDb(dir string) (*Db, error) {
	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	db := &Db{
		outPath: outputPath,
		out:     f,
		index:   make(hashIndex),
	}
	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

const bufSize = 8192

func (db *Db) recover() error {
	input, err := os.Open(db.outPath)
	if err != nil {
		return err
	}
	defer input.Close()

	in := bufio.NewReaderSize(input, bufSize)
	for {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(4)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = make([]byte, size)
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if n != int(size) {
			return fmt.Errorf("corrupted file")
		}

		var record Entry
		err = record.Deserialize(data)
		if err != nil {
			return err
		}
		db.index[record.Key] = db.outOffset
		db.outOffset += int64(n)
	}
	return nil
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	position, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}

	file, err := os.Open(db.outPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := retrieveValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) Put(key, value string) error {
	record := Entry{
		Key:   key,
		Value: value,
	}
	data, err := record.Serialize()
	if err != nil {
		return err
	}
	n, err := db.out.Write(data)
	if err == nil {
		db.index[key] = db.outOffset
		db.outOffset += int64(n)
	}
	return err
}

package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
)

type Entry struct {
	Key, Value string
}

func NewDataRecord(key, value string) *Entry {
	return &Entry{Key: key, Value: value}
}

func calculateLength(key, value string) int64 {
	return int64(len(key) + len(value) + 12)
}

func (r *Entry) Serialize() ([]byte, error) {
	kl := len(r.Key)
	vl := len(r.Value)
	size := kl + vl + 12
	res := make([]byte, size)
	binary.LittleEndian.PutUint32(res, uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], r.Key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(vl))
	copy(res[kl+12:], r.Value)
	return res, nil
}

func (r *Entry) GetLength() int64 {
	return calculateLength(r.Key, r.Value)
}

func (r *Entry) Deserialize(input []byte) error {
	if len(input) < 12 {
		return fmt.Errorf("input data too short")
	}
	kl := binary.LittleEndian.Uint32(input[4:])
	if len(input) < int(kl)+12 {
		return fmt.Errorf("input data too short for key")
	}
	keyBuf := make([]byte, kl)
	copy(keyBuf, input[8:kl+8])
	r.Key = string(keyBuf)
	vl := binary.LittleEndian.Uint32(input[kl+8:])
	if len(input) < int(kl)+12+int(vl) {
		return fmt.Errorf("input data too short for value")
	}
	valBuf := make([]byte, vl)
	copy(valBuf, input[kl+12:kl+12+vl])
	r.Value = string(valBuf)
	return nil
}

func retrieveValue(in *bufio.Reader) (string, error) {
	header, err := in.Peek(8)
	if err != nil {
		return "", err
	}
	keySize := int(binary.LittleEndian.Uint32(header[4:]))
	_, err = in.Discard(keySize + 8)
	if err != nil {
		return "", err
	}
	header, err = in.Peek(4)
	if err != nil {
		return "", err
	}
	valSize := int(binary.LittleEndian.Uint32(header))
	_, err = in.Discard(4)
	if err != nil {
		return "", err
	}
	data := make([]byte, valSize)
	n, err := in.Read(data)
	if err != nil {
		return "", err
	}
	if n != valSize {
		return "", fmt.Errorf("can't read value bytes (read %d, expected %d)", n, valSize)
	}
	return string(data), nil
}

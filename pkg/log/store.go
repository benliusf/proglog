package log

// The file store for a segment; we continually append records or read a record
// from the offset.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	// Number of bytes used to store the record's length
	lenOffset = 8
)

type store struct {
	*os.File
	mu     sync.Mutex
	buf    *bufio.Writer
	size   uint64
	logger Logger
}

func newStore(f *os.File, logger Logger) (*store, error) {
	if f == nil {
		return nil, fmt.Errorf("nil file")
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File:   f,
		size:   size,
		buf:    bufio.NewWriter(f),
		logger: logger,
	}, nil
}

func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenOffset
	s.size += uint64(w)
	if s.logger != nil {
		s.logger.Trace("Written %d bytes to file=%s; file size=%d", w, s.File.Name(), s.size)
	}
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenOffset)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	n, err := s.File.ReadAt(b, int64(lenOffset+pos))
	if err != nil {
		return nil, err
	}
	if s.logger != nil {
		s.logger.Trace("Read %d bytes at position %d from file=%s", n, pos, s.File.Name())
	}
	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}

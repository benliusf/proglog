package log

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
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

	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File, bufSize uint64) (*store, error) {
	if f == nil {
		return nil, fmt.Errorf("nil file")
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		buf:  bufio.NewWriterSize(f, int(bufSize)),
		size: size,
	}, nil
}

func (s *store) write(p []byte) (n uint64, pos uint64, err error) {
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
	return uint64(w), pos, nil
}

func (s *store) flush() error {
	return s.buf.Flush()
}

func (s *store) read(pos uint64) ([]byte, error) {
	if err := s.flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenOffset)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(lenOffset+pos)); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *store) readAt(b []byte, off int64) (int, error) {
	if err := s.flush(); err != nil {
		return -1, err
	}
	return s.File.ReadAt(b, off)
}

func (s *store) close() error {
	if err := s.flush(); err != nil {
		return err
	}
	return s.File.Close()
}

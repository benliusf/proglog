package log

import (
	"fmt"
	"os"
	"path"
)

const (
	ext = ".store"
)

type segment struct {
	uid uint64

	store *store

	maxBytes uint64
}

func newSegment(uid uint64, conf Config) (*segment, error) {
	s := &segment{
		uid:      uid,
		maxBytes: conf.Segment.MaxStoreBytes,
	}
	pref := conf.Log.Prefix
	if pref != "" {
		pref += "."
	}
	f, err := os.OpenFile(
		path.Join(conf.Log.Dir, fmt.Sprintf("%s%d%s", pref, uid, ext)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(f, conf.Segment.BufferBytes); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func (s *segment) isMaxed() bool {
	return s.store.size >= s.maxBytes
}

func (s *segment) append(data []byte) error {
	if _, _, err := s.store.write(data); err != nil {
		return err
	}
	return nil
}

func (s *segment) flush() error {
	return s.store.flush()
}

func (s *segment) close() error {
	return s.store.close()
}

func (s *segment) remove() error {
	if err := s.close(); err != nil {
		return err
	}
	return os.Remove(s.store.Name())
}

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

	maxBytes uint64
	store    *store
}

func newSegment(uid uint64, conf Config) (*segment, error) {
	s := &segment{
		uid:      uid,
		maxBytes: conf.Segment.MaxStoreBytes,
	}
	var err error
	pref := conf.Log.Prefix
	if pref != "" {
		pref += "."
	}
	storeFile, err := os.OpenFile(
		path.Join(conf.Log.Dir, fmt.Sprintf("%s%d%s", pref, uid, ext)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
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

func (s *segment) close() error {
	if err := s.store.close(); err != nil {
		return err
	}
	return nil
}

func (s *segment) remove() error {
	if err := s.close(); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

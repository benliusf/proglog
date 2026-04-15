package log

import (
	"io"
	"os"
)

type Iter struct {
	log *Log

	index int

	curr *store
	pos  uint64
}

func newIter(l *Log) (*Iter, error) {
	iter := &Iter{
		log: l,
	}
	return iter, iter.open()
}

func (i *Iter) open() error {
	if i.index >= len(i.log.segments) {
		return io.EOF
	}
	s := i.log.segments[i.index]
	f, err := os.OpenFile(s.store.Name(), os.O_RDWR, 0444)
	if err != nil {
		return err
	}
	if i.curr, err = newStore(f, 0); err != nil {
		defer f.Close()
		return err
	}
	i.pos = 0
	return nil
}

func (i *Iter) rotate() error {
	if i.curr != nil {
		if err := i.curr.Close(); err != nil {
			return err
		}
	}
	i.index++
	if err := i.open(); err != nil {
		return err
	}
	return nil
}

func (i *Iter) HasNext() bool {
	if i.curr != nil {
		return (i.pos < i.curr.size) ||
			(i.index+1 < len(i.log.segments))
	}
	return false
}

func (i *Iter) Next() ([]byte, error) {
	data, err := i.curr.read(i.pos)
	if err != nil {
		return nil, err
	}
	i.pos += uint64(lenOffset + len(data))
	if i.pos >= i.curr.size {
		if err := i.rotate(); err != nil && err != io.EOF {
			return nil, err
		}
	}
	return data, nil
}

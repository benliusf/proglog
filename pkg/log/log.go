package log

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Log struct {
	Config Config

	mu sync.Mutex

	activeSegment *segment
	segments      []*segment

	buf  chan []byte
	errs chan *LogError

	w *worker

	closed atomic.Bool
}

func NewLog(c Config) (*Log, error) {
	if c.Log.Dir == "" {
		return nil, fmt.Errorf("must specify directory!")
	}
	l := &Log{
		Config: c,
	}
	if l.Config.Segment.MaxStoreBytes == 0 {
		l.Config.Segment.MaxStoreBytes = 1024
	}
	if l.Config.Buffer.Size == 0 {
		l.Config.Buffer.Size = 1_000
	}
	if l.Config.Buffer.Timeout <= 0 {
		l.Config.Buffer.Timeout = 10 * time.Second
	}
	return l, l.setup()
}

func (l *Log) setup() error {
	dir, err := os.Open(l.Config.Log.Dir)
	if err != nil {
		return err
	}
	match := func(f fs.DirEntry) bool {
		if f.IsDir() {
			return false
		}
		return (l.Config.Log.Prefix == "" || strings.HasPrefix(f.Name(), l.Config.Log.Prefix)) &&
			strings.HasSuffix(f.Name(), ext)
	}
	parse := func(f fs.DirEntry) (uint64, error) {
		tmp := f.Name()
		tmp = strings.TrimSuffix(tmp, path.Ext(tmp))
		if strings.Contains(tmp, ".") {
			tmp = tmp[strings.LastIndex(tmp, ".")+1:]
		}
		uid, err := strconv.ParseUint(tmp, 10, 0)
		if err != nil {
			return 0, fmt.Errorf("unrecognized log file `%v`: %w", f.Name(), err)
		}
		return uid, nil
	}
	var uids []uint64
	for {
		files, err := dir.ReadDir(100)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		for _, f := range files {
			if match(f) {
				uid, err := parse(f)
				if err != nil {
					return err
				}
				uids = append(uids, uid)
			}
		}
	}
	sort.Slice(uids, func(i, j int) bool {
		return uids[i] < uids[j]
	})
	for i := 0; i < len(uids); i++ {
		if err = l.newSegment(uids[i]); err != nil {
			return err
		}
	}
	if l.segments == nil {
		if err = l.newSegment(1); err != nil {
			return err
		}
	}
	l.buf = make(chan []byte, l.Config.Buffer.Size)
	l.errs = l.Config.Errors
	l.w = &worker{
		log:  l,
		done: make(chan struct{}),
	}
	go l.w.run()
	return nil
}

func (l *Log) newSegment(uid uint64) error {
	s, err := newSegment(uid, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

func (l *Log) Append(ctx context.Context, data []byte) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("context is closed")
	case <-time.After(l.Config.Buffer.Timeout):
		return fmt.Errorf("timed out")
	case l.buf <- data:
	}
	return nil
}

func (l *Log) Iter() (*Iter, error) {
	return newIter(l)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed.Load() {
		return nil
	}
	close(l.buf)
	l.w.flush()
	l.closed.Store(true)
	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	for _, s := range l.segments {
		if err := s.remove(); err != nil {
			return err
		}
	}
	l.activeSegment = nil
	l.segments = []*segment{}
	return nil
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	l.closed.Swap(false)
	return l.setup()
}

func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if lowest >= l.activeSegment.uid {
		return fmt.Errorf("cannot remove active segment")
	}
	var segments []*segment
	for _, s := range l.segments {
		if s.uid <= lowest {
			if err := s.remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

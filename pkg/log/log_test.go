package log

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	dir, err := os.MkdirTemp("", "log-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	errs := make(chan *LogError, 100)
	log, err := NewLog(Config{
		Log: struct {
			Dir    string
			Prefix string
		}{
			Dir:    dir,
			Prefix: "test",
		},
		Buffer: struct {
			Size    uint64
			Timeout time.Duration
		}{
			Size: 1024 * 1024,
		},
		Errors: errs,
	})
	require.NoError(t, err)
	testAppend(t, log)
	close(errs)
	require.Equal(t, 0, len(errs))
}

func testAppend(t *testing.T, log *Log) {
	t.Helper()
	ctx := context.TODO()

	data := []byte(`Hold fast to dreams
For if dreams die
Life is a broken-winged bird
That cannot fly.

Hold fast to dreams
For when dreams go
Life is a barren field
Frozen with snow.

	- Langston Hughes`)

	n := 3
	for i := 0; i < n; i++ {
		err := log.Append(ctx, data)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	f, _, err := openFile(log.activeSegment.store.File.Name())
	require.NoError(t, err)
	s, err := newStore(f)
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		pos := uint64(0) * uint64(lenOffset+len(data))
		b, err := s.read(pos)
		require.NoError(t, err)
		require.Equal(t, data, b)
	}
}

func TestTruncate(t *testing.T) {
	dir, err := os.MkdirTemp("", "truncate-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	conf := Config{
		Log: struct {
			Dir    string
			Prefix string
		}{
			Dir: dir,
		},
	}

	n := 3
	for i := 0; i < n; i++ {
		_, err := newSegment(uint64(i+1), conf)
		require.NoError(t, err)
	}
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Equal(t, n, len(files))

	log, err := NewLog(conf)
	require.NoError(t, err)
	require.Equal(t, n, len(log.segments))

	require.NoError(t, log.Truncate(uint64(n-1)))
	require.Equal(t, 1, len(log.segments))
	files, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))

	require.NoError(t, log.Close())
}

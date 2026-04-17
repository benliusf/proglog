package log

import (
	"context"
	"crypto/rand"
	"fmt"
	math "math/rand/v2"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIter(t *testing.T) {
	data := []byte(`If you can't fly then run,
if you can't run then walk,
if you can't walk then crawl,
but whatever you do you have to keep moving forward.
`)
	dir, err := os.MkdirTemp("", "iter-test")
	require.NoError(t, err)

	log, err := NewLog(Config{
		Log: struct {
			Dir    string
			Prefix string
		}{
			Dir: dir,
		},
	})
	defer log.Remove()

	iter, err := log.Iter()
	require.NoError(t, err)
	require.False(t, iter.HasNext())

	n := 10
	for i := 0; i < n; i++ {
		log.Append(context.TODO(), data)
	}
	log.Close()

	iter, err = log.Iter()
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		require.True(t, iter.HasNext())
		b, err := iter.Next()
		require.NoError(t, err)
		require.Equal(t, data, b)
	}
	require.False(t, iter.HasNext())
}

func BenchmarkIter(b *testing.B) {
	dir, err := os.MkdirTemp("", "benchmark-iter")
	require.NoError(b, err)

	ctx := context.TODO()

	log, err := NewLog(Config{
		Log: struct {
			Dir    string
			Prefix string
		}{
			Dir: dir,
		},
		Segment: struct {
			MaxStoreBytes uint64
			BufferBytes   uint64
		}{
			MaxStoreBytes: 1024 * 1024 * 1000,
		},
	})
	defer log.Remove()

	for {
		data := make([]byte, math.IntN(4096-32)+32)
		_, err := rand.Read(data)
		require.NoError(b, err)
		if (log.activeSegment.store.size + uint64(len(data)) + lenOffset) > (1024 * 1024 * 100) {
			break
		}
		require.NoError(b, log.Append(ctx, data))
	}
	log.Close()

	require.Equal(b, 1, len(log.segments))

	file := log.activeSegment.store
	b.Run(fmt.Sprintf("File( file=%s, size=%d)", file.Name(), file.size),
		func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				iter, err := log.Iter()
				require.NoError(b, err)
				for iter.HasNext() {
					_, err := iter.Next()
					require.NoError(b, err)
				}
			}
			b.StopTimer()
		})
}

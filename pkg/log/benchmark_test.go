package log

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func benchmarkAppend(b *testing.B, conf Config) {
	data := []byte(`If I can stop one heart from breaking,
I shall not live in vain;
If I can ease one life the aching,
Or cool one pain,
Or help one fainting robin
Unto his nest again,
I shall not live in vain.

	- Emily Dickinson`)

	dir, err := os.MkdirTemp("", "benchmark-test")
	require.NoError(b, err)
	defer os.RemoveAll(dir)

	conf.Log.Dir = dir

	log, err := NewLog(conf)
	require.NoError(b, err)
	defer log.Close()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := log.Append(ctx, data); err != nil {
			b.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		b.Error(err)
	}
	b.StopTimer()
	if err := log.Remove(); err != nil {
		b.Error(err)
	}
}

func BenchmarkLog(b *testing.B) {
	defaultConf := Config{
		Buffer: struct {
			Size    uint64
			Timeout time.Duration
		}{
			Timeout: 1 * time.Second,
		},
	}
	tests := []struct {
		segmentMaxStoreBytes uint64
		segmentBufferBytes   uint64
		bufferSize           uint64
	}{
		{
			segmentMaxStoreBytes: 1024 * 1024 * 25,
			segmentBufferBytes:   1024 * 1024,
			bufferSize:           1_000,
		},
		{
			segmentMaxStoreBytes: 1024 * 1024 * 100,
			segmentBufferBytes:   1024 * 1024 * 2,
			bufferSize:           10_000,
		},
		{
			segmentMaxStoreBytes: 1024 * 1024 * 1000,
			segmentBufferBytes:   1024 * 1024,
			bufferSize:           25_000,
		},
	}
	for _, tt := range tests {
		copy := defaultConf
		copy.Segment.MaxStoreBytes = tt.segmentMaxStoreBytes
		copy.Segment.BufferBytes = tt.segmentBufferBytes
		copy.Buffer.Size = tt.bufferSize
		b.Run(fmt.Sprintf("Config( MaxStoreBytes=%v, BufferBytes=%v, BufferSize=%v )",
			copy.Segment.MaxStoreBytes, copy.Segment.BufferBytes, copy.Buffer.Size),
			func(b *testing.B) {
				benchmarkAppend(b, copy)
			})
	}
}

package log

import "time"

type Config struct {
	Log struct {
		Dir    string
		Prefix string
	}
	Segment struct {
		MaxStoreBytes uint64
		BufferBytes   uint64
	}
	Buffer struct {
		Size    uint64
		Timeout time.Duration
	}
	Errors chan *LogError
}

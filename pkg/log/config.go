package log

type Config struct {
	Log struct {
		Dir    string
		Prefix string
	}
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}

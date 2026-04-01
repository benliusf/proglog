package log

type Logger interface {
	Info(msg string, args ...any)
	Debug(msg string, args ...any)
	Trace(msg string, args ...any)
}

package log

import (
	stdLog "log"
	"os"
)

type testLogger struct {
	info  stdLog.Logger
	debug stdLog.Logger
	trace stdLog.Logger
}

func (l *testLogger) Info(format string, args ...any) {
	l.info.Printf(format, args...)
}

func (l *testLogger) Debug(format string, args ...any) {
	l.debug.Printf(format, args...)
}

func (l *testLogger) Trace(format string, args ...any) {
	l.trace.Printf(format, args...)
}

func newTestLogger() *testLogger {
	return &testLogger{
		info:  *stdLog.New(os.Stdout, "[INFO] ", stdLog.Ldate|stdLog.Ltime|stdLog.Lmsgprefix),
		debug: *stdLog.New(os.Stdout, "[DEBUG] ", stdLog.Ldate|stdLog.Ltime|stdLog.Lmsgprefix),
		trace: *stdLog.New(os.Stdout, "[TRACE] ", stdLog.Ldate|stdLog.Ltime|stdLog.Lmsgprefix),
	}
}

package log

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	stdlog "log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type severity int32

const (
	DEBUG severity = iota
	INFO
	WARNING
	ERROR
)

var names = []string{
	DEBUG:   "DEBUG",
	INFO:    "INFO",
	WARNING: "WARNING",
	ERROR:   "ERROR",
}

func (s *severity) Get() interface{} {
	return *s
}

func (s *severity) Set(value string) error {
	threshold := INFO
	value = strings.ToUpper(value)
	for i, name := range names {
		if name == value {
			threshold = severity(i)
		}
	}
	*s = threshold
	return nil
}

func (s *severity) String() string {
	return names[int(*s)]
}

type logger struct {
	sync.Mutex
	buffer *buffer

	// *stdlog.Logger
	debug   *stdlog.Logger
	info    *stdlog.Logger
	warning *stdlog.Logger
	err     *stdlog.Logger

	severity severity
	dir      string
}

type buffer struct {
	bytes.Buffer
	next *buffer
}

func (l *logger) getBuffer() *buffer {
	l.Lock()
	b := l.buffer
	if b != nil {
		l.buffer = b.next
	}
	l.Unlock()
	if b == nil {
		b = new(buffer)
	} else {
		b.next = nil
		b.Reset()
	}
	return b
}

func (l *logger) print(args ...interface{}) {
	buf := l.getBuffer()
	fmt.Fprint(buf, args...)
	if buf.Bytes()[buf.Len()-1] != '\n' {
		buf.WriteByte('\n')
	}
}

func (l *logger) printf(format string, args ...interface{}) {
	buf := l.getBuffer()
	fmt.Fprintf(buf, format, args...)
}

// the default logger
var log logger

func init() {
	flag.StringVar(&log.dir, "log_dir", "", "if empty, write log files in this directory")
	flag.Var(&log.severity, "log_level", "logs at and above this level")

	format := stdlog.Ldate | stdlog.Ltime | stdlog.Lmicroseconds | stdlog.Lshortfile
	log.debug = stdlog.New(os.Stdout, "[DEBUG] ", format)
	log.info = stdlog.New(os.Stdout, "[INFO] ", format)
	log.warning = stdlog.New(os.Stderr, "[WARNING] ", format)
	log.err = stdlog.New(os.Stderr, "[ERROR] ", format)
}

// Setup setup log format and output file
func Setup() {
	format := stdlog.Ldate | stdlog.Ltime | stdlog.Lmicroseconds | stdlog.Lshortfile
	fname := fmt.Sprintf("%s.%d.log", filepath.Base(os.Args[0]), os.Getpid())
	f, err := os.Create(filepath.Join(log.dir, fname))
	if err != nil {
		stdlog.Fatal(err)
	}
	log.debug = stdlog.New(f, "[DEBUG] ", format)
	log.info = stdlog.New(f, "[INFO] ", format)
	multi := io.MultiWriter(f, os.Stderr)
	log.warning = stdlog.New(multi, "[WARNING] ", format)
	log.err = stdlog.New(multi, "[ERROR] ", format)
}

func Debug(v ...interface{}) {
	if log.severity == DEBUG {
		log.debug.Output(2, fmt.Sprint(v...))
	}
}

func Debugf(format string, v ...interface{}) {
	if log.severity == DEBUG {
		log.debug.Output(2, fmt.Sprintf(format, v...))
	}
}

func Info(v ...interface{}) {
	if log.severity <= INFO {
		log.info.Output(2, fmt.Sprint(v...))
	}
}

func Infof(format string, v ...interface{}) {
	if log.severity <= INFO {
		log.info.Output(2, fmt.Sprintf(format, v...))
	}
}

func Warning(v ...interface{}) {
	if log.severity <= WARNING {
		log.warning.Output(2, fmt.Sprint(v...))
	}
}

func Warningf(format string, v ...interface{}) {
	if log.severity <= WARNING {
		log.warning.Output(2, fmt.Sprintf(format, v...))
	}
}

func Error(v ...interface{}) {
	log.err.Output(2, fmt.Sprint(v...))
}

func Errorf(format string, v ...interface{}) {
	log.err.Output(2, fmt.Sprintf(format, v...))
}

func Fatal(v ...interface{}) {
	log.err.Output(2, fmt.Sprint(v...))
	stdlog.Fatal(v...)
}

func Fatalf(format string, v ...interface{}) {
	log.err.Output(2, fmt.Sprintf(format, v...))
	stdlog.Fatalf(format, v...)
}

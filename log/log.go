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
	threshold := DEBUG
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
	sync.Once

	sync.Mutex
	buffer *buffer

	*stdlog.Logger
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

func (l *logger) println(args ...interface{}) {
	buf := l.getBuffer()
	fmt.Fprintln(buf, args...)
}

func (l *logger) printf(format string, args ...interface{}) {
	buf := l.getBuffer()
	fmt.Fprintf(buf, format, args...)
}

// the default logger
var log logger

func init() {
	flag.StringVar(&log.dir, "log_dir", "", "if non-empty, write log files in this directory")
	flag.Var(&log.severity, "log_level", "logs at and above this level")
}

func setup() {
	if !flag.Parsed() {
		os.Stderr.Write([]byte("ERROR: logging before flag.Parse: "))
		flag.Parse()
	}

	format := stdlog.Ldate | stdlog.Ltime | stdlog.Lmicroseconds | stdlog.Lshortfile
	if log.dir != "" {
		program := filepath.Base(os.Args[0])
		pid := os.Getpid()
		fname := fmt.Sprintf("%s.%d.log", program, pid)
		path := filepath.Join(log.dir, fname)
		f, err := os.Create(path)
		if err != nil {
			stdlog.Fatal(err)
		}
		log.debug = stdlog.New(f, "[DEBUG] ", format)
		log.info = stdlog.New(f, "[INFO] ", format)
		multi := io.MultiWriter(f, os.Stderr)
		log.warning = stdlog.New(multi, "[WARNING] ", format)
		log.err = stdlog.New(multi, "[ERROR] ", format)
	} else {
		log.debug = stdlog.New(os.Stdout, "[DEBUG] ", format)
		log.info = stdlog.New(os.Stdout, "[INFO] ", format)
		log.warning = stdlog.New(os.Stderr, "[WARNING] ", format)
		log.err = stdlog.New(os.Stderr, "[ERROR] ", format)
	}
}

func Debug(v ...interface{}) {
	log.Once.Do(setup)
	if log.severity == DEBUG {
		log.debug.Output(2, fmt.Sprint(v...))
	}
}

func Debugln(v ...interface{}) {
	log.Once.Do(setup)
	if log.severity == DEBUG {
		log.debug.Output(2, fmt.Sprintln(v...))
	}
}

func Debugf(format string, v ...interface{}) {
	log.Once.Do(setup)
	if log.severity == DEBUG {
		log.debug.Output(2, fmt.Sprintf(format, v...))
	}
}

func Info(v ...interface{}) {
	log.Once.Do(setup)
	if log.severity <= INFO {
		log.info.Output(2, fmt.Sprint(v...))
	}
}

func Infoln(v ...interface{}) {
	log.Once.Do(setup)
	if log.severity <= INFO {
		log.info.Output(2, fmt.Sprintln(v...))
	}
}

func Infof(format string, v ...interface{}) {
	log.Once.Do(setup)
	if log.severity <= INFO {
		log.info.Output(2, fmt.Sprintf(format, v...))
	}
}

func Warning(v ...interface{}) {
	log.Once.Do(setup)
	if log.severity <= WARNING {
		log.warning.Output(2, fmt.Sprint(v...))
	}
}

func Warningln(v ...interface{}) {
	log.Once.Do(setup)
	if log.severity <= WARNING {
		log.warning.Output(2, fmt.Sprintln(v...))
	}
}

func Warningf(format string, v ...interface{}) {
	log.Once.Do(setup)
	if log.severity <= WARNING {
		log.warning.Output(2, fmt.Sprintf(format, v...))
	}
}

func Error(v ...interface{}) {
	log.Once.Do(setup)
	log.err.Output(2, fmt.Sprint(v...))
}

func Errorln(v ...interface{}) {
	log.Once.Do(setup)
	log.err.Output(2, fmt.Sprintln(v...))
}

func Errorf(format string, v ...interface{}) {
	log.Once.Do(setup)
	log.err.Output(2, fmt.Sprintf(format, v...))
}

func Fatal(v ...interface{}) {
	log.Once.Do(setup)
	log.err.Output(2, fmt.Sprint(v...))
	stdlog.Fatal(v)
}

func Fatalln(v ...interface{}) {
	log.Once.Do(setup)
	log.err.Output(2, fmt.Sprintln(v...))
	stdlog.Fatalln(v)
}

func Fatalf(format string, v ...interface{}) {
	log.Once.Do(setup)
	log.err.Output(2, fmt.Sprintf(format, v...))
	stdlog.Fatalf(format, v)
}

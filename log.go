package barethoven

import (
	"fmt"
	stdlog "log"
	"os"
)

type Logger interface {
	Printf(format string, v ...interface{})
	Print(v ...interface{})
	Println(v ...interface{})
}

var log Logger = stdlog.New(os.Stderr, "", stdlog.LstdFlags)

func SetPackageLogger(l Logger) {
	log = l
}

type socketLogger struct {
	logSlug      string
	logSlugSlice []interface{}
}

func newSocketLogger(host, console string) *socketLogger {
	return &socketLogger{
		fmt.Sprintf("[bsock] (host: %s) (console: %s) ", host, console),
		[]interface{}{fmt.Sprintf("[bsock] (host: %s) (console: %s) ", host, console)},
	}
}

func (l *socketLogger) Printf(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s%s", l.logSlug, format), v...)
}
func (l *socketLogger) Print(v ...interface{}) {
	log.Print(append(l.logSlugSlice, v...)...)
}
func (l *socketLogger) Println(v ...interface{}) {
	log.Println(append(l.logSlugSlice, v...)...)
}

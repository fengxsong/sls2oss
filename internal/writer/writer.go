package writer

import (
	"errors"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

const (
	defaultSize = 256
	megabyte    = 1024 * 1024
)

// very simple file rotate writer
type RotateWriter struct {
	// config
	pattern             string
	maxSize             int
	maxAge              time.Duration
	closeInactive       time.Duration
	scanInterval        time.Duration
	asyncRotateCallback func(string)
	// runtime infos
	quit      <-chan struct{}
	size      int64    // current size
	fn        string   // store current filename with time
	file      *os.File // file holder
	createdAt time.Time
	logger    log.Logger
	mu        sync.Mutex
}

type Option func(*RotateWriter)

func WithMaxSize(size int) Option {
	return func(w *RotateWriter) {
		w.maxSize = size
	}
}

func WithMaxAge(maxAge time.Duration) Option {
	return func(w *RotateWriter) {
		w.maxAge = maxAge
	}
}

func WithCloseInactive(d time.Duration) Option {
	return func(w *RotateWriter) {
		w.closeInactive = d
	}
}

func WithScanInterval(d time.Duration) Option {
	return func(w *RotateWriter) {
		w.scanInterval = d
	}
}

func WithLogger(logger log.Logger) Option {
	return func(w *RotateWriter) {
		w.logger = logger
	}
}

func WithAsyncRotateCallback(cb func(string)) Option {
	return func(w *RotateWriter) {
		w.asyncRotateCallback = cb
	}
}

type nopLogger struct{}

func (l *nopLogger) Log(_ ...interface{}) error { return nil }

func New(pattern string, quit <-chan struct{}, opts ...Option) (*RotateWriter, error) {
	if pattern == "" {
		return nil, errors.New("pattern must not been null")
	}
	w := &RotateWriter{
		pattern:       pattern,
		maxSize:       defaultSize * megabyte,
		closeInactive: time.Minute,
		scanInterval:  time.Second,
		quit:          quit,
		logger:        &nopLogger{},
	}
	for _, opt := range opts {
		opt(w)
	}

	go w.loop()
	return w, nil
}

func (w *RotateWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	writeLen := int64(len(p))
	if writeLen > w.max() {
		return 0, errors.New("write length exceed")
	}
	if w.file == nil {
		if err = w.openFile(writeLen); err != nil {
			return 0, err
		}
	}

	if w.size+writeLen > int64(w.max()) {
		if err = w.rotate(); err != nil {
			return 0, err
		}
	}
	n, err = w.file.Write(p)
	w.size += int64(n)
	return n, nil
}

func (w *RotateWriter) openFile(writeLen int64) error {
	fn := w.filename()
	info, err := os.Stat(fn)
	if os.IsNotExist(err) {
		return w.openNew()
	}
	if err != nil {
		return err
	}
	if info.Size()+int64(writeLen) > int64(w.max()) {
		return w.rotate()
	}
	file, err := os.OpenFile(fn, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return w.openNew()
	}
	w.file = file
	w.size = info.Size()
	return nil
}

func (w *RotateWriter) loop() {
	ticker := time.NewTicker(w.scanInterval)
	for {
		select {
		case <-ticker.C:
			if w.file != nil && !w.createdAt.IsZero() {
				if info, err := w.file.Stat(); (err == nil && time.Since(info.ModTime()) > w.closeInactive) || time.Since(w.createdAt) > w.maxAge {
					w.mu.Lock()
					if err = w.close(); err != nil {
						level.Error(w.logger).Log("msg", "failed to close in interval", "err", err)
					}
					w.mu.Unlock()
				}
			}
		case <-w.quit:
			w.mu.Lock()
			if err := w.close(); err != nil {
				level.Error(w.logger).Log("msg", "failed to close", "err", err)
			}
			w.mu.Unlock()
			ticker.Stop()
			return
		}
	}
}

func (w *RotateWriter) rotate() error {
	if err := w.close(); err != nil {
		return err
	}
	return w.openNew()
}

func (w *RotateWriter) Closed() bool {
	return w.file == nil
}

func (w *RotateWriter) close() error {
	level.Debug(w.logger).Log("msg", "trying to close file", "path", w.filename())
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	if w.asyncRotateCallback != nil {
		go w.asyncRotateCallback(w.filename())
	}
	w.file = nil
	w.size = 0
	return err
}

func (w *RotateWriter) openNew() error {
	level.Debug(w.logger).Log("msg", "create new file")
	// reset filename
	w.fn = ""
	if err := os.MkdirAll(path.Dir(w.filename()), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(w.filename(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	w.file = f
	w.createdAt = time.Now()
	return nil
}

func (w *RotateWriter) filename() string {
	if w.fn == "" {
		w.fn = filepath.Join(os.TempDir(), w.pattern, strconv.Itoa(int(time.Now().Unix())))
	}
	return w.fn
}

func (w *RotateWriter) max() int64 {
	if w.maxSize == 0 {
		return int64(defaultSize * megabyte)
	}
	return int64(w.maxSize * megabyte)
}

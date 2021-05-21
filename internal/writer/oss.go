package writer

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"gl.weeget.cn/devops/sls2oss/internal/config"
	"gl.weeget.cn/devops/sls2oss/internal/metrics"
)

// oss writer wrap rotateWriter
type OssWriter struct {
	cfg    *config.OssConfig
	quit   <-chan struct{}
	logger log.Logger

	ossBucketClient *oss.Bucket
	// simple mutex to ensure thread safe
	files map[string]*RotateWriter
	wg    *sync.WaitGroup
	mu    sync.Mutex
}

func NewOssWriter(cfg *config.OssConfig, logger log.Logger, quit <-chan struct{}) (*OssWriter, error) {
	if logger == nil {
		logger = &nopLogger{}
	}
	w := &OssWriter{
		cfg:    cfg,
		quit:   quit,
		logger: logger,
		files:  make(map[string]*RotateWriter),
		wg:     &sync.WaitGroup{},
	}
	ossClient, err := oss.New(w.cfg.Endpoint, w.cfg.AccessKeyID, w.cfg.AccessKeySecret)
	if err != nil {
		return nil, err
	}
	w.ossBucketClient, err = ossClient.Bucket(w.cfg.Bucket)
	if err != nil {
		return nil, err
	}
	go w.loop()
	return w, nil
}

// clean file holder
func (w *OssWriter) loop() {
	ticker := time.NewTicker(time.Duration(w.cfg.ScanInterval))
	for range ticker.C {
		for n, rw := range w.files {
			if rw.Closed() {
				w.mu.Lock()
				delete(w.files, n)
				w.mu.Unlock()
			}
		}
	}
}

// todo or fix: ensure wait happend after rotate writer close
func (w *OssWriter) Wait() error {
	<-w.quit
	// very simple trick :)
	time.Sleep(time.Second)
	w.wg.Wait()
	level.Info(w.logger).Log("msg", "about to exit oss writer")
	return nil
}

// todo: limit number of rotateWriters, too many goroutines may cause panic
func (w *OssWriter) get(path string) (*RotateWriter, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	rw, ok := w.files[path]
	if !ok {
		var err error
		// todo: check if argument is valid
		rw, err = New(path, w.quit,
			WithMaxSize(w.cfg.MaxSize),
			WithMaxAge(time.Duration(w.cfg.MaxAge)),
			WithScanInterval(time.Duration(w.cfg.ScanInterval)),
			WithCloseInactive(time.Duration(w.cfg.CloseInactive)),
			WithLogger(w.logger),
			WithAsyncRotateCallback(w.send))
		if err != nil {
			return nil, err
		}
		w.files[path] = rw
	}
	return rw, nil
}

func (w *OssWriter) WriteTo(path string, data []byte) (n int, err error) {
	rw, err := w.get(path)
	if err != nil {
		return 0, err
	}
	return rw.Write(data)
}

var bufPool = sync.Pool{
	New: func() interface{} { return &bytes.Buffer{} },
}

func (w *OssWriter) send(path string) {
	level.Debug(w.logger).Log("sendfile", path)
	w.wg.Add(1)
	defer w.wg.Done()

	if w.ossBucketClient == nil {
		level.Warn(w.logger).Log("msg", "null oss bucket client")
		return
	}
	var err error
	defer func() {
		if err == nil {
			os.Remove(path)
			level.Debug(w.logger).Log("msg", "remove file", "path", path)
		}
	}()

	ossOptions := []oss.Option{}
	if w.cfg.StorageClassType != "" {
		ossOptions = append(ossOptions, oss.ObjectStorageClass(oss.StorageClassType(w.cfg.StorageClassType)))
	}

	if w.cfg.Compress {
		buf := bufPool.Get().(*bytes.Buffer)
		defer func() {
			buf.Reset()
			bufPool.Put(buf)
		}()
		gw, err := gzip.NewWriterLevel(buf, w.cfg.CompressLevel)
		if err != nil {
			level.Error(w.logger).Log("msg", "create gzip writer", "err", err)
			return
		}
		fp, err := os.Open(path)
		if err != nil {
			level.Error(w.logger).Log("msg", "open file", "err", err)
			return
		}
		defer fp.Close()
		io.Copy(gw, fp)
		if err = gw.Close(); err != nil {
			level.Error(w.logger).Log("msg", "close gzipwriter", "err", err)
			return
		}
		gzFile := path + ".gz"
		if err = ioutil.WriteFile(gzFile, buf.Bytes(), 0644); err != nil {
			level.Error(w.logger).Log("msg", "write gzip file", "err", err)
			return
		}
		defer os.Remove(gzFile)

		objectKey := getObjectKeyFromPath(gzFile)
		level.Debug(w.logger).Log("msg", "put object file", "object", objectKey, "gzfile", gzFile)
		if err = w.ossBucketClient.PutObjectFromFile(objectKey, gzFile, ossOptions...); err != nil {
			level.Error(w.logger).Log("msg", "send objectfile", "err", err)
			return
		}
		metrics.PipelineWriteBytesTotal.WithLabelValues(getTopicFromObjectKey(objectKey), "oss", "gzip").Add(float64(buf.Len()))
		return
	}
	objectKey := getObjectKeyFromPath(path)
	level.Debug(w.logger).Log("msg", "put object file", "object", objectKey, "file", path)
	if err = w.ossBucketClient.PutObjectFromFile(objectKey, path, ossOptions...); err != nil {
		level.Error(w.logger).Log("msg", "send objectfile", "err", err)
	}
}

func getTopicFromObjectKey(s string) string {
	return strings.Split(s, string(os.PathSeparator))[0]
}

func getObjectKeyFromPath(s string) string {
	return strings.TrimPrefix(strings.TrimPrefix(s, os.TempDir()), string(os.PathSeparator))
}

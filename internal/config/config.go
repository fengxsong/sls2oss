package config

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"runtime"
	"time"

	"sigs.k8s.io/yaml"
)

type Config struct {
	Input  *Input  `json:"input"`
	Filter *Filter `json:"filter,omitempty"`
	Output *Output `json:"output"`
	Metric *Metric `json:"metric,omitempty"`
	Worker int     `json:"worker,omitempty"`
}

type Input struct {
	Sls *SlsConfig `json:"sls"`
}

type Filter struct {
	//
}

type Output struct {
	Oss *OssConfig `json:"oss"`
}

type Metric struct {
	Port int    `json:"port"`
	Path string `json:"path"`
}

type SlsConfig struct {
	Endpoint              string   `json:"endpoint"`
	AccessKeyID           string   `json:"access_key"`
	AccessKeySecret       string   `json:"access_key_secret"`
	Project               string   `json:"project"`
	Logstores             []string `json:"logstores"`
	ConsumerGroupName     string   `json:"consumer_group"`
	ConsumerName          string   `json:"consumer_name,omitempty"`
	CursorPosition        string   `json:"cursor_position"`
	CursorStartTime       int64    `json:"cursor_start_time"` // unix second, unit is second
	DataFetchIntervalInMs int      `json:"fetch_interval_ms"` // default is 200
	MaxFetchLogGroupCount int      `json:"max_fetch_count"`   // max is 1000
	InOrder               bool     `json:"in_order"`
	IncludeMeta           bool     `json:"include_meta"`
}

// todo: validate and set defaults
func (c *SlsConfig) ValidateAndSetDefaults() error {
	return nil
}

type OssConfig struct {
	Endpoint         string   `json:"endpoint"`
	AccessKeyID      string   `json:"access_key"`
	AccessKeySecret  string   `json:"access_key_secret"`
	Bucket           string   `json:"bucket"`
	StorageClassType string   `json:"storage_class"`
	Compress         bool     `json:"compress"`
	CompressLevel    int      `json:"compress_level"`
	MaxSize          int      `json:"max_size"`
	MaxAge           Duration `json:"max_age"`
	CloseInactive    Duration `json:"close_inactive"`
	ScanInterval     Duration `json:"scan_interval"`
}

type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*d = Duration(time.Duration(value))
		return nil
	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = Duration(tmp)
		return nil
	default:
		return errors.New("invalid duration")
	}
}

func (c *OssConfig) ValidateAndSetDefaults() error {
	return nil
}

func ReadFromFile(path string) (*Config, error) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	data := os.ExpandEnv(string(content))
	var cfg Config
	err = yaml.Unmarshal([]byte(data), &cfg)
	return &cfg, err
}

func (c *Config) ValidateAndSetDefaults() error {
	if c.Input.Sls != nil {
		if err := c.Input.Sls.ValidateAndSetDefaults(); err != nil {
			return err
		}
	}
	if c.Output.Oss != nil {
		if err := c.Output.Oss.ValidateAndSetDefaults(); err != nil {
			return err
		}
	}
	if c.Worker == 0 {
		c.Worker = runtime.NumCPU()
	}
	return nil
}

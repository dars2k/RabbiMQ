package sdk

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config defines the SDK client configuration.
type Config struct {
	Address            string `json:"address" yaml:"address"`
	Timeout            string `json:"timeout" yaml:"timeout"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify" yaml:"insecure_skip_verify"`
	CACertFile         string `json:"ca_cert_file" yaml:"ca_cert_file"`
	Passphrase         string `json:"passphrase" yaml:"passphrase"`
	// Channel is used by Publish, Consume and Subscribe when they are called
	// with an empty channel name.
	Channel string `json:"channel" yaml:"channel"`
}

// Validate checks that the config can be used to build a client.
func (c Config) Validate() error {
	if c.Address == "" {
		return fmt.Errorf("address is required")
	}
	u, err := url.Parse(c.Address)
	if err != nil {
		return fmt.Errorf("invalid address %q: %w", c.Address, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid address %q: scheme must be http or https", c.Address)
	}
	if u.Host == "" {
		return fmt.Errorf("invalid address %q: missing host", c.Address)
	}
	if _, err := c.timeoutOrDefault(); err != nil {
		return err
	}
	return nil
}

// LoadConfig reads a JSON or YAML config file from disk.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := unmarshalConfig(path, data, &cfg); err != nil {
		return Config{}, err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}

	return cfg, nil
}

func (c Config) timeoutOrDefault() (time.Duration, error) {
	if c.Timeout == "" {
		return 10 * time.Second, nil
	}
	d, err := time.ParseDuration(c.Timeout)
	if err != nil {
		return 0, fmt.Errorf("invalid timeout %q: %w", c.Timeout, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("invalid timeout %q: must not be negative", c.Timeout)
	}
	return d, nil
}

func unmarshalConfig(path string, data []byte, out interface{}) error {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, out); err != nil {
			return fmt.Errorf("yaml decode failed: %w", err)
		}
		return nil
	case ".json", "":
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("json decode failed: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported config extension: %s", ext)
	}
}

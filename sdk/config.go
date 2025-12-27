package sdk

import (
	"encoding/json"
	"fmt"
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

	if cfg.Address == "" {
		return Config{}, fmt.Errorf("address is required")
	}

	return cfg, nil
}

func (c Config) timeoutOrDefault() (time.Duration, error) {
	if c.Timeout == "" {
		return 10 * time.Second, nil
	}
	return time.ParseDuration(c.Timeout)
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

package config

import (
	"RabbiMQ/broker"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ServerConfig defines broker/server settings.
type ServerConfig struct {
	// Addr is the listen address. Command-line flags take precedence.
	Addr string `json:"addr" yaml:"addr"`
	// DataFile is the log file for the default channel; other channels are
	// stored next to it. Command-line flags take precedence.
	DataFile   string        `json:"dataFile" yaml:"dataFile"`
	Passphrase string        `json:"passphrase" yaml:"passphrase"`
	Limits     broker.Config `json:"limits" yaml:"limits"`
}

// LoadServerConfig reads server configuration from disk.
func LoadServerConfig(path string) (ServerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ServerConfig{}, err
	}

	var cfg ServerConfig
	if err := unmarshalConfig(path, data, &cfg); err != nil {
		return ServerConfig{}, err
	}
	if err := cfg.Limits.Validate(); err != nil {
		return ServerConfig{}, fmt.Errorf("%s: limits: %w", path, err)
	}
	return cfg, nil
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

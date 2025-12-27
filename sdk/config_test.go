package sdk

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.json")
	content := `{"address":"http://localhost:8080","timeout":"2s","insecure_skip_verify":true,"ca_cert_file":"","passphrase":"secret"}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Address != "http://localhost:8080" {
		t.Fatalf("unexpected address: %s", cfg.Address)
	}
	if cfg.Passphrase != "secret" {
		t.Fatalf("unexpected passphrase: %s", cfg.Passphrase)
	}
}

func TestLoadConfigYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")
	content := "address: http://localhost:8080\npassphrase: secret\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Address != "http://localhost:8080" || cfg.Passphrase != "secret" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

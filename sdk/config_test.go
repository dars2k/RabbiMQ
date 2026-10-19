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

func TestLoadConfigErrors(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		return path
	}

	cases := map[string]string{
		"missing file":    filepath.Join(dir, "missing.json"),
		"bad json":        write("bad.json", "{"),
		"bad yaml":        write("bad.yaml", "address: [unclosed"),
		"unsupported ext": write("cfg.toml", "address = 'x'"),
		"missing address": write("noaddr.json", `{"timeout":"1s"}`),
		"invalid timeout": write("timeout.yml", "address: http://h\ntimeout: soon\n"),
		"invalid scheme":  write("scheme.json", `{"address":"tcp://h"}`),
	}
	for name, path := range cases {
		if _, err := LoadConfig(path); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}

	if _, err := NewClientFromFile(filepath.Join(dir, "missing.json")); err == nil {
		t.Fatal("expected error from NewClientFromFile")
	}
	path := write("ok.yaml", "address: http://localhost:8080\nchannel: orders\n")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Channel != "orders" {
		t.Fatalf("unexpected channel: %q", cfg.Channel)
	}
	if _, err := NewClientFromFile(path); err != nil {
		t.Fatal(err)
	}
}

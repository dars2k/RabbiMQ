package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadServerConfigYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "server.yaml")
	content := "passphrase: secret\nlimits:\n  maxQueueDepth: 10\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadServerConfig(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Passphrase != "secret" {
		t.Fatalf("unexpected passphrase: %s", cfg.Passphrase)
	}
	if cfg.Limits.MaxQueueDepth != 10 {
		t.Fatalf("unexpected limit: %d", cfg.Limits.MaxQueueDepth)
	}
}

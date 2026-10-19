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

func TestLoadServerConfigJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "server.json")
	content := `{"addr":"0.0.0.0:9000","dataFile":"/data/q.log","limits":{"maxConsumers":3}}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadServerConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "0.0.0.0:9000" || cfg.DataFile != "/data/q.log" || cfg.Limits.MaxConsumers != 3 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestLoadServerConfigErrors(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	cases := map[string]string{
		"missing":        filepath.Join(dir, "missing.yaml"),
		"bad yaml":       write("bad.yml", "limits: [1"),
		"bad json":       write("bad.json", "{"),
		"unsupported":    write("cfg.ini", "x=1"),
		"negative limit": write("neg.yaml", "limits:\n  maxQueueDepth: -5\n"),
	}
	for name, path := range cases {
		if _, err := LoadServerConfig(path); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// The example configs shipped in the repository must stay loadable.
func TestExampleConfigs(t *testing.T) {
	for _, path := range []string{
		"../examples/simple/rabbimq.yaml",
		"../examples/docker/rabbimq.yaml",
	} {
		if _, err := LoadServerConfig(path); err != nil {
			t.Errorf("%s: %v", path, err)
		}
	}
}

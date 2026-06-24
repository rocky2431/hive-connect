package main

import (
	"strings"
	"testing"
)

func TestHiveAPIURLBuildsPrefixedEndpoint(t *testing.T) {
	got, err := hiveAPIURL("https://hive.example/", "/api", "/local-bridge/status")
	if err != nil {
		t.Fatalf("hiveAPIURL returned error: %v", err)
	}
	if got != "https://hive.example/api/local-bridge/status" {
		t.Fatalf("url = %q", got)
	}
}

func TestDefaultHiveURLUsesProductionWebOrigin(t *testing.T) {
	if defaultHiveURL != "https://frontend-production-0346.up.railway.app" {
		t.Fatalf("defaultHiveURL = %q", defaultHiveURL)
	}
}

func TestRenderHiveConnectConfigUsesHivePlatformOnly(t *testing.T) {
	cfg := renderHiveConnectConfig(hiveLoginConfig{
		ProjectName: "codex-on-mac",
		AgentType:   "codex",
		WorkDir:     "/Users/rocky/workspace",
		BackendURL:  "https://hive.example",
		APIPrefix:   "/api",
		Token:       "hb_secret",
		RuntimeKind: "codex",
		DeviceName:  "Rocky Mac",
	})

	for _, want := range []string{
		`name = "codex-on-mac"`,
		`type = "codex"`,
		`work_dir = "/Users/rocky/workspace"`,
		`type = "hive"`,
		`backend_url = "https://hive.example"`,
		`token = "hb_secret"`,
		`runtime_kind = "codex"`,
	} {
		if !strings.Contains(cfg, want) {
			t.Fatalf("config missing %q:\n%s", want, cfg)
		}
	}
	if strings.Contains(cfg, "feishu") || strings.Contains(cfg, "telegram") || strings.Contains(cfg, "slack") {
		t.Fatalf("config should only include Hive platform:\n%s", cfg)
	}
}

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

func TestDefaultHiveURLsSplitWebActivationAndBackendRuntime(t *testing.T) {
	if defaultHiveWebURL != "https://frontend-production-0346.up.railway.app" {
		t.Fatalf("defaultHiveWebURL = %q", defaultHiveWebURL)
	}
	if defaultHiveBackendURL != "https://backend-production-326d.up.railway.app" {
		t.Fatalf("defaultHiveBackendURL = %q", defaultHiveBackendURL)
	}
}

func TestResolveHiveLoginOriginsDefaultsToWebFrontendAndBackendAPI(t *testing.T) {
	origins := resolveHiveLoginOrigins("", "", "")
	if origins.WebURL != defaultHiveWebURL {
		t.Fatalf("WebURL = %q", origins.WebURL)
	}
	if origins.BackendURL != defaultHiveBackendURL {
		t.Fatalf("BackendURL = %q", origins.BackendURL)
	}
}

func TestResolveHiveLoginOriginsLegacyURLOverridesBoth(t *testing.T) {
	origins := resolveHiveLoginOrigins("https://hive.example", "https://web.example", "https://api.example")
	if origins.WebURL != "https://hive.example" || origins.BackendURL != "https://hive.example" {
		t.Fatalf("origins = %#v", origins)
	}
}

func TestHiveVerificationURLUsesConfiguredWebOrigin(t *testing.T) {
	got := hiveVerificationURL(
		hivePairingInitResponse{
			UserCode:                "HIVE-ABCD-1234",
			VerificationURIComplete: "https://backend.example/local-bridge/activate?user_code=HIVE-ABCD-1234",
		},
		"https://frontend.example/",
	)
	want := "https://frontend.example/local-bridge/activate?user_code=HIVE-ABCD-1234"
	if got != want {
		t.Fatalf("verification url = %q, want %q", got, want)
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

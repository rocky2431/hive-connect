package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	hiveConnectClientKind = "hive-connect"
	hiveConnectAPIPrefix  = "/api"
)

type hiveLoginConfig struct {
	ProjectName string
	AgentType   string
	WorkDir     string
	BackendURL  string
	APIPrefix   string
	Token       string
	RuntimeKind string
	DeviceName  string
	DataDir     string
}

type hiveConnectSession struct {
	BackendURL   string `json:"backend_url"`
	APIPrefix    string `json:"api_prefix"`
	Token        string `json:"token"`
	AgentType    string `json:"agent_type"`
	ProjectName  string `json:"project_name"`
	WorkDir      string `json:"work_dir"`
	RuntimeKind  string `json:"runtime_kind"`
	DeviceName   string `json:"device_name"`
	ConnectionID string `json:"connection_id,omitempty"`
	AgentID      string `json:"agent_id,omitempty"`
	TenantID     string `json:"tenant_id,omitempty"`
	UserID       string `json:"user_id,omitempty"`
	UpdatedAt    string `json:"updated_at"`
}

type hivePairingInitResponse struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	ExpiresIn               int    `json:"expires_in"`
	Interval                int    `json:"interval"`
	PairingID               string `json:"pairing_id"`
}

type hivePairingExchangeResponse struct {
	Status       string `json:"status"`
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ConnectionID string `json:"connection_id"`
	AgentID      string `json:"agent_id"`
	TenantID     string `json:"tenant_id"`
	UserID       string `json:"user_id"`
	Interval     int    `json:"interval"`
}

func rewriteHiveRunArgs(args []string) []string {
	rest := append([]string{}, args[2:]...)
	if hasConfigArg(rest) {
		return append([]string{args[0]}, rest...)
	}
	configPath := defaultHiveConnectConfigPath()
	if _, err := os.Stat(configPath); err != nil {
		fmt.Fprintf(os.Stderr, "Hive Connect config not found: %s\n", configPath)
		fmt.Fprintln(os.Stderr, "Run `hive-connect login --hive-url <your Hive URL>` first.")
		os.Exit(1)
	}
	return append([]string{args[0], "--config", configPath}, rest...)
}

func hasConfigArg(args []string) bool {
	for i, arg := range args {
		if arg == "--config" || arg == "-config" {
			return i+1 < len(args)
		}
		if strings.HasPrefix(arg, "--config=") || strings.HasPrefix(arg, "-config=") {
			return true
		}
	}
	return false
}

func runHiveLogin(args []string) {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	hiveURL := fs.String("hive-url", firstHiveNonEmpty(os.Getenv("HIVE_BACKEND_URL"), os.Getenv("HIVE_URL")), "Hive web or backend origin")
	apiPrefix := fs.String("api-prefix", firstHiveNonEmpty(os.Getenv("HIVE_API_PREFIX"), hiveConnectAPIPrefix), "Hive API prefix")
	agentType := fs.String("agent", firstHiveNonEmpty(os.Getenv("HIVE_AGENT_TYPE"), "codex"), "Local agent runtime type: codex, claudecode, cursor, gemini, opencode, qoder, iflow")
	workDir := fs.String("work-dir", defaultHiveWorkDir(), "Workspace directory passed to the local agent")
	projectName := fs.String("project", "", "Local Hive Connect project name")
	deviceName := fs.String("device-name", defaultHiveDeviceName(), "Device name shown in Hive")
	noBrowser := fs.Bool("no-browser", false, "Print activation URL instead of opening the browser")
	timeout := fs.Duration("timeout", 15*time.Minute, "Maximum time to wait for browser approval")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: hive-connect login --hive-url https://your-hive.example.com [options]")
		fs.PrintDefaults()
	}
	_ = fs.Parse(args)

	if strings.TrimSpace(*hiveURL) == "" {
		fmt.Fprintln(os.Stderr, "Error: --hive-url is required. Use the copied setup instruction from Hive.")
		os.Exit(1)
	}

	session, err := performHiveLogin(context.Background(), hiveLoginConfig{
		ProjectName: firstHiveNonEmpty(*projectName, defaultHiveProjectName(*agentType, *deviceName)),
		AgentType:   strings.TrimSpace(*agentType),
		WorkDir:     strings.TrimSpace(*workDir),
		BackendURL:  strings.TrimRight(strings.TrimSpace(*hiveURL), "/"),
		APIPrefix:   normalizeHiveAPIPrefix(*apiPrefix),
		RuntimeKind: firstHiveNonEmpty(strings.TrimSpace(*agentType), hiveConnectClientKind),
		DeviceName:  strings.TrimSpace(*deviceName),
		DataDir:     defaultHiveConnectDataDir(),
	}, *noBrowser, *timeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Hive Connect login failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Hive Connect login complete.\nConfig: %s\nRun:    hive-connect run\nStatus: hive-connect status\n", defaultHiveConnectConfigPath())
	if session.AgentID != "" {
		fmt.Printf("Agent:  %s\n", session.AgentID)
	}
}

func runHiveStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	hiveURL := fs.String("hive-url", "", "Override Hive web or backend origin")
	apiPrefix := fs.String("api-prefix", "", "Override Hive API prefix")
	token := fs.String("token", "", "Override hb_* token")
	_ = fs.Parse(args)

	session, _ := readHiveConnectSession()
	backendURL := firstHiveNonEmpty(*hiveURL, session.BackendURL, os.Getenv("HIVE_BACKEND_URL"), os.Getenv("HIVE_URL"))
	prefix := firstHiveNonEmpty(*apiPrefix, session.APIPrefix, os.Getenv("HIVE_API_PREFIX"), hiveConnectAPIPrefix)
	bridgeToken := firstHiveNonEmpty(*token, session.Token, os.Getenv("HIVE_CONNECT_TOKEN"), os.Getenv("HIVE_BRIDGE_TOKEN"))
	if backendURL == "" || bridgeToken == "" {
		fmt.Fprintln(os.Stderr, "Hive Connect is not logged in. Run `hive-connect login --hive-url <your Hive URL>` first.")
		os.Exit(1)
	}
	statusURL, err := hiveAPIURL(backendURL, prefix, "/local-bridge/status")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid Hive URL: %v\n", err)
		os.Exit(1)
	}
	req, err := http.NewRequest(http.MethodGet, statusURL, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Create status request failed: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Authorization", "Bearer "+bridgeToken)
	req.Header.Set("User-Agent", "hive-connect")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Hive Connect status failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "Hive Connect status failed: status=%d body=%s\n", resp.StatusCode, strings.TrimSpace(string(body)))
		os.Exit(1)
	}
	fmt.Println(strings.TrimSpace(string(body)))
}

func performHiveLogin(ctx context.Context, cfg hiveLoginConfig, noBrowser bool, timeout time.Duration) (hiveConnectSession, error) {
	initURL, err := hiveAPIURL(cfg.BackendURL, cfg.APIPrefix, "/local-bridge/pairing/init")
	if err != nil {
		return hiveConnectSession{}, err
	}
	initPayload := map[string]any{
		"device_name":        cfg.DeviceName,
		"client_kind":        hiveConnectClientKind,
		"device_fingerprint": hiveDeviceFingerprint(cfg.WorkDir, cfg.DeviceName),
		"scopes": []string{
			"local_agent:connect",
			"local_agent:receive",
			"local_agent:send",
			"local_agent:report",
			"presence:write",
			"gateway:poll",
			"gateway:report",
			"gateway:send-message",
			"files:upload",
		},
	}
	var initOut hivePairingInitResponse
	if err := hiveJSON(ctx, http.MethodPost, initURL, "", initPayload, &initOut); err != nil {
		return hiveConnectSession{}, err
	}
	if initOut.DeviceCode == "" || initOut.VerificationURIComplete == "" {
		return hiveConnectSession{}, fmt.Errorf("pairing init response missing device_code or verification_uri_complete")
	}

	fmt.Printf("Opening Hive authentication page:\n  %s\n", initOut.VerificationURIComplete)
	if noBrowser {
		fmt.Println("Browser auto-open disabled. Open the URL above to approve this local agent.")
	} else if err := openBrowser(initOut.VerificationURIComplete); err != nil {
		fmt.Printf("Could not open browser automatically: %v\nOpen the URL above manually.\n", err)
	}

	exchangeURL, err := hiveAPIURL(cfg.BackendURL, cfg.APIPrefix, "/local-bridge/pairing/exchange")
	if err != nil {
		return hiveConnectSession{}, err
	}
	interval := time.Duration(firstPositive(initOut.Interval, 3)) * time.Second
	expires := time.Duration(firstPositive(initOut.ExpiresIn, int(timeout.Seconds()))) * time.Second
	if timeout > 0 && timeout < expires {
		expires = timeout
	}
	deadline := time.Now().Add(expires)
	for {
		var exchange hivePairingExchangeResponse
		err := hiveJSON(ctx, http.MethodPost, exchangeURL, "", map[string]string{"device_code": initOut.DeviceCode}, &exchange)
		if err == nil && exchange.AccessToken != "" {
			session := hiveConnectSession{
				BackendURL:   cfg.BackendURL,
				APIPrefix:    cfg.APIPrefix,
				Token:        exchange.AccessToken,
				AgentType:    cfg.AgentType,
				ProjectName:  cfg.ProjectName,
				WorkDir:      cfg.WorkDir,
				RuntimeKind:  cfg.RuntimeKind,
				DeviceName:   cfg.DeviceName,
				ConnectionID: exchange.ConnectionID,
				AgentID:      exchange.AgentID,
				TenantID:     exchange.TenantID,
				UserID:       exchange.UserID,
				UpdatedAt:    time.Now().UTC().Format(time.RFC3339),
			}
			cfg.Token = exchange.AccessToken
			if err := writeHiveConnectSession(session); err != nil {
				return hiveConnectSession{}, err
			}
			if err := writeHiveConnectConfig(cfg); err != nil {
				return hiveConnectSession{}, err
			}
			return session, nil
		}
		if err != nil && !strings.Contains(err.Error(), `"status":"pending"`) {
			return hiveConnectSession{}, err
		}
		if time.Now().After(deadline) {
			return hiveConnectSession{}, fmt.Errorf("timed out waiting for Hive approval")
		}
		time.Sleep(interval)
	}
}

func hiveJSON(ctx context.Context, method string, rawURL string, token string, in any, out any) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "hive-connect")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return err
	}
	return nil
}

func writeHiveConnectConfig(cfg hiveLoginConfig) error {
	if err := os.MkdirAll(filepath.Dir(defaultHiveConnectConfigPath()), 0o700); err != nil {
		return err
	}
	return os.WriteFile(defaultHiveConnectConfigPath(), []byte(renderHiveConnectConfig(cfg)), 0o600)
}

func renderHiveConnectConfig(cfg hiveLoginConfig) string {
	dataDir := firstHiveNonEmpty(cfg.DataDir, defaultHiveConnectDataDir())
	return strings.Join([]string{
		"# Hive Connect configuration",
		"",
		"data_dir = " + strconv.Quote(dataDir),
		"",
		"[log]",
		`level = "info"`,
		"",
		"[[projects]]",
		"name = " + strconv.Quote(cfg.ProjectName),
		"",
		"[projects.agent]",
		"type = " + strconv.Quote(cfg.AgentType),
		"",
		"[projects.agent.options]",
		"work_dir = " + strconv.Quote(cfg.WorkDir),
		"",
		"[[projects.platforms]]",
		`type = "hive"`,
		"",
		"[projects.platforms.options]",
		"backend_url = " + strconv.Quote(cfg.BackendURL),
		"token = " + strconv.Quote(cfg.Token),
		"api_prefix = " + strconv.Quote(firstHiveNonEmpty(cfg.APIPrefix, hiveConnectAPIPrefix)),
		"runtime_kind = " + strconv.Quote(firstHiveNonEmpty(cfg.RuntimeKind, hiveConnectClientKind)),
		"device_name = " + strconv.Quote(cfg.DeviceName),
		`allow_from = "*"`,
		"",
	}, "\n")
}

func writeHiveConnectSession(session hiveConnectSession) error {
	if err := os.MkdirAll(defaultHiveConnectDir(), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(session, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(defaultHiveConnectSessionPath(), data, 0o600)
}

func readHiveConnectSession() (hiveConnectSession, error) {
	var session hiveConnectSession
	data, err := os.ReadFile(defaultHiveConnectSessionPath())
	if err != nil {
		return session, err
	}
	err = json.Unmarshal(data, &session)
	return session, err
}

func hiveAPIURL(baseURL string, apiPrefix string, endpoint string) (string, error) {
	u, err := url.Parse(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("Hive URL must use http or https, got %q", u.Scheme)
	}
	u.Path = path.Join(u.Path, normalizeHiveAPIPrefix(apiPrefix), endpoint)
	return u.String(), nil
}

func normalizeHiveAPIPrefix(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return hiveConnectAPIPrefix
	}
	if !strings.HasPrefix(value, "/") {
		value = "/" + value
	}
	return strings.TrimRight(value, "/")
}

func defaultHiveConnectDir() string {
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".hive-connect")
	}
	return ".hive-connect"
}

func defaultHiveConnectDataDir() string {
	return filepath.Join(defaultHiveConnectDir(), "data")
}

func defaultHiveConnectConfigPath() string {
	return filepath.Join(defaultHiveConnectDir(), "config.toml")
}

func defaultHiveConnectSessionPath() string {
	return filepath.Join(defaultHiveConnectDir(), "connection.json")
}

func defaultHiveWorkDir() string {
	if wd, err := os.Getwd(); err == nil {
		return wd
	}
	return "."
}

func defaultHiveDeviceName() string {
	host, _ := os.Hostname()
	if strings.TrimSpace(host) != "" {
		return host
	}
	return "Local Agent"
}

func defaultHiveProjectName(agentType string, deviceName string) string {
	base := strings.TrimSpace(deviceName)
	if base == "" {
		base = "local-agent"
	}
	agentType = strings.TrimSpace(agentType)
	if agentType == "" {
		return base
	}
	return agentType + "-" + strings.ReplaceAll(base, " ", "-")
}

func hiveDeviceFingerprint(workDir string, deviceName string) string {
	host, _ := os.Hostname()
	home, _ := os.UserHomeDir()
	sum := sha256.Sum256([]byte(host + "\n" + home + "\n" + workDir + "\n" + deviceName))
	return hex.EncodeToString(sum[:])
}

func firstHiveNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstPositive(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

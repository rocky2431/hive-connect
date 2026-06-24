# Hive Connect Hive Platform Adapter

Hive Connect makes the local process act as an outbound WebSocket client:

1. It uses the `hb_*` bridge token to request a short-lived WebSocket ticket.
2. It dials Hive Cloud at `/local-bridge/channel/ws`.
3. Hive sends channel messages as `message` frames.
4. The local agent reply is sent back as durable Hive channel `event` frames.

No inbound local port or reverse proxy is required.

## Minimal Config

```toml
[[projects]]
name = "codex-on-mac"

[projects.agent]
type = "codex"

[projects.agent.options]
work_dir = "/Users/you/workspace"

[[projects.platforms]]
type = "hive"

[projects.platforms.options]
backend_url = "${HIVE_BACKEND_URL}"
token = "${HIVE_CONNECT_TOKEN}"
api_prefix = "/api"
runtime_kind = "codex"
device_name = "Mac Codex"
allow_from = "*"
```

`backend_url` should be the Hive backend or public app origin that serves the
`/api/local-bridge/*` routes. `token` is the long-lived `hb_*` token returned by
`hive-connect login`.

## Run With Only Hive Platform

```bash
HIVE_BACKEND_URL="https://your-hive.example.com" \
HIVE_CONNECT_TOKEN="hb_xxx" \
go run -tags 'no_web no_feishu no_telegram no_discord no_slack no_dingtalk no_wecom no_weixin no_qq no_qqbot no_line no_weibo no_max no_matrix no_webex no_wps_xiezuo' ./cmd/cc-connect run --config config.toml
```

## Current Contract

- Hive -> local: WebSocket `message` frames become `core.Message`.
- Local -> Hive text: `event` frames with `event_type = "text"`.
- Local -> Hive files/images: uploads to `/local-bridge/upload` when possible,
  then emits `file` or `image` events with artifact metadata.
- Presence: the WebSocket `ready` frame marks the runner online in Hive.
- Auth: all HTTP calls use `Authorization: Bearer <hb_token>`.

Streaming preview still needs a Hive-side update/delta event contract before it
can behave exactly like mature IM editing.

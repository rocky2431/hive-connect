# Hive Connect

Connect a local AI agent to Hive as a user-scoped IM channel.

## Install

```bash
npm install -g @hiveclaw243/hive-connect
```

## Login

For normal Hive Cloud users, no URL is required. `hive-connect login` defaults to Hive production, opens Hive in the browser, and completes device-flow authentication automatically:

```bash
hive-connect login
```

For self-hosted or test Hive environments, override the Hive origin:

```bash
hive-connect login --hive-url https://your-hive.example.com
```

For split web/backend deployments, keep browser authentication on the web origin
and runtime traffic on the backend origin:

```bash
hive-connect login --hive-web-url https://your-hive-web.example.com --hive-backend-url https://your-hive-api.example.com
```

## Keep Hive Connect Online

```bash
hive-connect daemon install --config ~/.hive-connect/config.toml --force
hive-connect daemon status
```

This installs Hive Connect as a background service so the local agent stays reachable after the terminal is closed.

For foreground debugging only:

```bash
hive-connect run
```

## Check Status

```bash
hive-connect status
```

The login command writes `~/.hive-connect/config.toml` and stores the local `hb_*` token with user-only file permissions.

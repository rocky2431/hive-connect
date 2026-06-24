# Hive Connect

Connect a local AI agent to Hive as a user-scoped IM channel.

## Install

```bash
npm install -g @hiveclaw243/hive-connect
```

## Login

Copy the setup instruction from Hive. It opens Hive in the browser and completes device-flow authentication automatically:

```bash
hive-connect login --hive-url https://your-hive.example.com
```

## Run

```bash
hive-connect run
```

## Check Status

```bash
hive-connect status
```

The login command writes `~/.hive-connect/config.toml` and stores the local `hb_*` token with user-only file permissions.

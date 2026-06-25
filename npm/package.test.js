#!/usr/bin/env node

"use strict";

const assert = require("assert");
const fs = require("fs");
const path = require("path");

const root = __dirname;
const pkg = require(path.join(root, "package.json"));
const installJs = fs.readFileSync(path.join(root, "install.js"), "utf8");
const runJs = fs.readFileSync(path.join(root, "run.js"), "utf8");
const readme = fs.readFileSync(path.join(root, "README.md"), "utf8");

assert.strictEqual(pkg.name, "@hiveclaw243/hive-connect");
assert.strictEqual(pkg.version, "0.1.6");
assert.deepStrictEqual(pkg.bin, { "hive-connect": "run.js" });
assert.strictEqual(pkg.repository.url, "git+https://github.com/rocky2431/hive-connect.git");

assert.match(installJs, /const NAME = "hive-connect"/);
assert.match(installJs, /const GITHUB_REPO = "rocky2431\/hive-connect"/);
assert.match(installJs, /User-Agent": "hive-connect-npm"/);
assert.doesNotMatch(installJs, /\[cc-connect]/);

assert.match(runJs, /const NAME = "hive-connect"/);
assert.match(runJs, /npm uninstall -g @hiveclaw243\/hive-connect && npm install -g @hiveclaw243\/hive-connect/);
assert.doesNotMatch(runJs, /\[cc-connect]/);

assert.match(readme, /^# Hive Connect/m);
assert.match(readme, /npm install -g @hiveclaw243\/hive-connect/);
assert.match(readme, /hive-connect login/);
assert.match(readme, /defaults to Hive production/);
assert.match(readme, /For self-hosted or test Hive environments/);
assert.match(readme, /hive-connect login --hive-url https:\/\/your-hive\.example\.com/);
assert.match(readme, /hive-connect login --hive-web-url https:\/\/your-hive-web\.example\.com --hive-backend-url https:\/\/your-hive-api\.example\.com/);
assert.match(readme, /hive-connect daemon install --config ~\/\.hive-connect\/config\.toml --force/);
assert.match(readme, /hive-connect daemon status/);
assert.match(readme, /background service/);
assert.doesNotMatch(readme, /For foreground debugging only/);
assert.doesNotMatch(readme, /hive-connect run/);
assert.doesNotMatch(readme, /cc-connect/);

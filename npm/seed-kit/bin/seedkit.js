#!/usr/bin/env node

const { execFileSync } = require("child_process");
const { chmodSync } = require("fs");

const PLATFORMS = {
  "linux-x64": "@seed-kit/cli-linux-x64",
  "linux-arm64": "@seed-kit/cli-linux-arm64",
  "darwin-x64": "@seed-kit/cli-darwin-x64",
  "darwin-arm64": "@seed-kit/cli-darwin-arm64",
  "win32-x64": "@seed-kit/cli-win32-x64",
};

const key = `${process.platform}-${process.arch}`;
const pkg = PLATFORMS[key];
if (!pkg) {
  console.error(
    `seedkit: unsupported platform ${process.platform}-${process.arch}\n` +
      `Supported: ${Object.keys(PLATFORMS).join(", ")}`
  );
  process.exit(1);
}

const ext = process.platform === "win32" ? ".exe" : "";
const bin = require.resolve(`${pkg}/seedkit${ext}`);

// npm strips executable bits from tarballs on unix
if (process.platform !== "win32") {
  chmodSync(bin, 0o755);
}

try {
  execFileSync(bin, process.argv.slice(2), { stdio: "inherit" });
} catch (e) {
  if (e.status !== null) {
    process.exit(e.status);
  }
  throw e;
}

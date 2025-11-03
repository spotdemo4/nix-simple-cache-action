import { writeFileSync } from "node:fs";
import * as cache from "@actions/cache";
import * as core from "@actions/core";
import * as exec from "@actions/exec";
import * as github from "@actions/github";
import { startServer } from "./util.js";

async function main() {
	// make sure caching is available
	if (!cache.isFeatureAvailable()) {
		core.warning("cache is not available");
		return;
	}

	// get nix version
	const version = (
		await exec.getExecOutput("nix", ["--version"], {
			silent: true,
		})
	).stdout.trim();
	core.info(`nix version: ${version}`);

	// get node version
	const nodeVersion = process.version;
	core.info(`node version: ${nodeVersion}`);

	// get flake hash
	const flakeHash = (
		await exec.getExecOutput(
			"nix",
			["hash", "file", "--type", "sha1", "--base64", "flake.nix"],
			{
				silent: true,
			},
		)
	).stdout.trim();
	core.info(`flake hash: ${flakeHash}`);
	core.saveState("flake-hash", flakeHash);

	// get lock hash
	const lockHash = (
		await exec.getExecOutput(
			"nix",
			["hash", "file", "--type", "sha1", "--base64", "flake.lock"],
			{
				silent: true,
			},
		)
	).stdout.trim();
	core.info(`lock hash: ${lockHash}`);
	core.saveState("lock-hash", lockHash);

	// restore cache to tmp
	const restore = await cache.restoreCache(
		["/tmp/nix-cache", "/tmp/.secret-key"],
		`nix-cache-${flakeHash}-${lockHash}-${github.context.job}`,
		[
			`nix-cache-${flakeHash}-${lockHash}`,
			`nix-cache-${flakeHash}`,
			`nix-cache`,
		],
	);
	if (restore === `nix-store-${flakeHash}-${lockHash}`) {
		core.saveState("hit-type", "direct");
		core.info("cache restored (direct hit)");
		core.setOutput("cache-hit", "true");
	} else if (restore) {
		core.saveState("hit-type", "indirect");
		core.info("cache restored (indirect hit)");
		core.setOutput("cache-hit", "true");
	} else {
		core.saveState("hit-type", "none");
		core.info("cache not found");
		core.setOutput("cache-hit", "false");

		// generate store secret key
		const secretKey = (
			await exec.getExecOutput(
				"nix",
				["key", "generate-secret", "--key-name", "simple.cache.action-1"],
				{ silent: true },
			)
		).stdout.trim();

		// write to file
		writeFileSync("/tmp/.secret-key", secretKey);
	}

	// get public key
	const publicKey = (
		await exec.getExecOutput(
			"bash",
			["-c", "cat /tmp/.secret-key | nix key convert-secret-to-public"],
			{
				silent: true,
			},
		)
	).stdout.trim();
	core.info(`public key: ${publicKey}`);
	core.saveState("public-key", publicKey);

	// start proxy server and get pid
	const pid = await startServer("5001", "/tmp/nix-cache");
	if (!pid) {
		core.warning("failed to start proxy server");
		return;
	}
	core.saveState("pid", pid.toString());

	// add cache as a substituter
	core.exportVariable(
		"NIX_CONFIG",
		`
			extra-substituters = http://127.0.0.1:5001?priority=50
			extra-trusted-public-keys = ${publicKey}
		`,
	);
}

try {
	await main();
} catch (error) {
	if (error instanceof Error) core.setFailed(error.message);
}

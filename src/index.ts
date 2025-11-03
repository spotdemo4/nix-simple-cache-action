import { spawn } from "node:child_process";
import { existsSync, openSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import * as cache from "@actions/cache";
import * as core from "@actions/core";
import * as exec from "@actions/exec";

async function main() {
	// make sure caching is available
	if (!cache.isFeatureAvailable()) {
		core.warning("cache is not available");
		return;
	}

	// print nix version
	const version = (
		await exec.getExecOutput("nix", ["--version"], {
			silent: true,
		})
	).stdout.trim();
	core.info(`nix version: ${version}`);

	// print node version
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
	core.saveState("flakeHash", flakeHash);

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
	core.saveState("lockHash", lockHash);

	// restore cache to tmp
	const restore = await cache.restoreCache(
		["/tmp/nix-cache", "/tmp/.secret-key"],
		`nix-store-${flakeHash}-${lockHash}`,
		[`nix-store-${flakeHash}`, "nix-store"],
	);
	if (restore) {
		core.info("cache restored");

		core.setOutput("cache-hit", "true");
		core.saveState(
			"directHit",
			restore === `nix-store-${flakeHash}-${lockHash}`,
		);
	} else {
		core.info("cache not found, generating cache secret key");

		core.setOutput("cache-hit", "false");
		core.saveState("directHit", "false");

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
	core.saveState("publicKey", publicKey);

	// determine path to proxy.js
	const __filename = fileURLToPath(import.meta.url);
	const __dirname = dirname(__filename);
	if (!existsSync(`${__dirname}/proxy.js`)) {
		core.warning(
			`${__dirname}/proxy.js not found, skipping binary cache server`,
		);
		return;
	}

	// create HTTP binary cache proxy server
	core.info("starting binary cache proxy server");
	const out = openSync("/tmp/out.log", "as"); // Open file for stdout
	const err = openSync("/tmp/err.log", "as"); // Open file for stderr
	const proxy = spawn("node", [`${__dirname}/proxy.js`], {
		detached: true,
		stdio: ["ignore", out, err],
	});
	core.saveState("proxyPID", proxy.pid?.toString());
	proxy.unref();

	// wait for the proxy server to start
	let ping = 1;
	let attempts = 0;
	while (ping !== 0 && attempts < 5) {
		ping = await exec.exec(
			"nix",
			["store", "info", "--store", "http://127.0.0.1:5001"],
			{ ignoreReturnCode: true, silent: true },
		);
		if (ping !== 0) {
			attempts++;
			core.info(`waiting for proxy server to start, attempt ${attempts}...`);
			await new Promise((resolve) => setTimeout(resolve, 1000));
		}
	}
	if (attempts >= 5) {
		core.warning("proxy server did not start.");
		core.warning(`stdout: ${readFileSync("/tmp/out.log", "utf8")}`);
		core.warning(`stderr: ${readFileSync("/tmp/err.log", "utf8")}`);
		return;
	}

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

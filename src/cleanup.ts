import * as cache from "@actions/cache";
import * as core from "@actions/core";
import * as exec from "@actions/exec";
import * as github from "@actions/github";
import { loadSubstituters, startServer, stopServer } from "./util.js";

async function main() {
	// get direct hit from state
	const hitType = core.getState("hit-type");
	switch (hitType) {
		case "direct":
			core.info("cache was a direct hit, skipping save");
			break;

		case "indirect": {
			core.info("cache was an indirect hit, creating new cache");

			const indirectPID = await startServer("5002", "/tmp/nix-cache");
			if (!indirectPID) {
				core.warning("failed to start proxy server");
				break;
			}

			await loadSubstituters("5002");
			await fixupStore();
			await copyTo("5002");
			await save();
			await stopServer(indirectPID, "5002");

			break;
		}

		default:
			core.info("cache was a miss, saving cache");

			await loadSubstituters("5001");
			await fixupStore();
			await copyTo("5001");
			await save();

			break;
	}

	// close proxy server
	const pid = core.getState("pid");
	await stopServer(parseInt(pid, 10), "5001");
}

// fixup nix store before copying
async function fixupStore() {
	// optimise
	core.info("optimising");
	await exec.exec("nix", ["store", "optimise"]);

	// sign
	core.info("signing");
	await exec.exec(
		"nix",
		["store", "sign", "--key-file", "/tmp/.secret-key", "--all"],
		{ silent: true },
	);

	// get public key from state
	const publicKey = core.getState("public-key");
	if (!publicKey) {
		core.warning("public key hash not found, not saving cache");
		return;
	}

	// verify
	core.info("verifying");
	await exec.exec(
		"nix",
		[
			"store",
			"verify",
			"--repair",
			"--trusted-public-keys",
			publicKey,
			"--all",
		],
		{
			silent: true,
		},
	);
}

// copy all store paths to proxy server
async function copyTo(port: string) {
	// add to cache
	const copy = await exec.exec(
		"nix",
		[
			"copy",
			"--to",
			`http://127.0.0.1:${port}?compression=zstd&parallel-compression=true`,
			"--connect-timeout",
			"60",
			"--keep-going",
			"--all",
		],
		{
			ignoreReturnCode: true,
		},
	);
	if (copy !== 0) {
		core.warning(`failed to copy some store paths (exit code ${copy})`);
	}
}

// save /tmp/nix-cache to action cache
async function save() {
	// make sure caching is available
	if (!cache.isFeatureAvailable()) {
		core.warning("cache is not available");
		return;
	}

	// get flake hash from state
	const flakeHash = core.getState("flake-hash");
	if (!flakeHash) {
		core.warning("flake hash not found, not saving cache");
		return;
	}

	// get lock hash from state
	const lockHash = core.getState("lock-hash");
	if (!lockHash) {
		core.warning("lock hash not found, not saving cache");
		return;
	}

	// save cache
	await cache.saveCache(
		["/tmp/nix-cache", "/tmp/.secret-key"],
		`nix-cache-${flakeHash}-${lockHash}-${github.context.job}`,
	);
}

try {
	await main();
} catch (error) {
	if (error instanceof Error) core.setFailed(error.message);
}

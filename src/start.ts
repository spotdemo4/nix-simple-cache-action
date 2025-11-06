import * as cache from "@actions/cache";
import * as core from "@actions/core";
import * as github from "@actions/github";
import * as nix from "./nix/nix.js";
import * as server from "./server/client.js";
import { cachePath, keyPath, port } from "./var.js";

async function main() {
	// make sure caching is available
	if (!cache.isFeatureAvailable()) {
		core.warning("cache is not available");
		return;
	}

	// get nix version
	const version = await nix.version();
	core.info(`nix version: ${version}`);

	// get node version
	const nodeVersion = process.version;
	core.info(`node version: ${nodeVersion}`);

	// get flake hash
	const flakeHash = await nix.hash("flake.nix");
	core.info(`flake hash: ${flakeHash}`);
	core.saveState("flake-hash", flakeHash);

	// get lock hash
	const lockHash = await nix.hash("flake.lock");
	core.info(`lock hash: ${lockHash}`);
	core.saveState("lock-hash", lockHash);

	// restore cache to tmp
	const restore = await cache.restoreCache(
		[cachePath, keyPath],
		`nix-cache-${flakeHash}-${lockHash}-${github.context.workflow}-${github.context.job}`,
		[
			`nix-cache-${flakeHash}-${lockHash}-${github.context.workflow}`,
			`nix-cache-${flakeHash}-${lockHash}`,
			`nix-cache-${flakeHash}`,
			`nix-cache`,
		],
	);
	if (
		restore ===
		`nix-cache-${flakeHash}-${lockHash}-${github.context.workflow}-${github.context.job}`
	) {
		core.info("cache restored (direct hit)");
		core.saveState("hit-type", "direct");
		core.setOutput("cache-hit", "true");
	} else if (restore) {
		core.info("cache restored (indirect hit)");
		core.saveState("hit-type", "indirect");
		core.setOutput("cache-hit", "true");
	} else {
		core.info("cache not found");
		core.saveState("hit-type", "none");
		core.setOutput("cache-hit", "false");

		// generate store secret key
		await nix.generateSecretKey();
	}

	// get public key
	const publicKey = await nix.getPublicKey();
	core.info(`public key: ${publicKey}`);
	core.saveState("public-key", publicKey);

	// start proxy server and get pid
	const pid = await server.start();
	if (!pid) {
		core.warning("failed to start proxy server");
		return;
	}
	core.saveState("pid", pid.toString());

	// add cache as a substituter
	core.exportVariable(
		"NIX_CONFIG",
		`
            extra-substituters = http://127.0.0.1:${port}?priority=0
            extra-trusted-public-keys = ${publicKey}
        `,
	);
}

try {
	await main();
} catch (error) {
	if (error instanceof Error) core.setFailed(error.message);
}

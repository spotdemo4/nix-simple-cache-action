import * as cache from "@actions/cache";
import * as core from "@actions/core";
import * as github from "@actions/github";
import * as io from "@actions/io";
import { request } from "undici";
import * as nix from "./nix/nix.js";
import * as server from "./server/client.js";
import { getTextBetween } from "./util.js";
import { cachePath, keyPath } from "./var.js";

async function main() {
	// make sure caching is available
	if (!cache.isFeatureAvailable()) {
		core.warning("cache is not available");
		return;
	}

	// get proxy server pid to make sure it's running
	const pidStr = core.getState("pid");
	if (!pidStr) {
		core.warning("no proxy server running");
		return;
	}
	const pid = parseInt(pidStr, 10);

	// get hit type
	const hitType = core.getState("hit-type");
	if (hitType === "direct") {
		core.info("cache was a direct hit, skipping save");
		await server.stop(pid);
		return;
	}

	// get nix store paths in local and cache
	const localPaths = await nix.store.list();
	const cachePaths = await nix.store.list(`file://${cachePath}`);

	// get all paths that are in local but not in cache
	const pathsToCheck = localPaths.filter((p) => !cachePaths.includes(p));
	core.info(`found ${pathsToCheck.length} paths to check against substituters`);

	// get all substituters
	const substituters = await nix.substituters();
	core.info(`substituters: ${substituters.join(", ")}`);

	// check all paths in parallel
	const pathsToCopyPromise = await Promise.allSettled(
		pathsToCheck.map(async (path) => {
			for (const sub of substituters) {
				const c = await check(path, sub);
				if (c) return null;
			}

			return path;
		}),
	);

	// filter out nulls
	const pathsToCopy = pathsToCopyPromise
		.filter((result) => result.status === "fulfilled" && result.value !== null)
		.map((result) => (result as PromiseFulfilledResult<string>).value);
	core.info(`found ${pathsToCopy.length} paths to copy to cache`);

	// copy paths to cache
	for (const path of pathsToCopy) {
		core.info(`copying ${path} to cache`);
		await nix.store.sign(path);
		await nix.store.copy(path, `file://${cachePath}`);
	}

	if (hitType === "none") {
		core.info("no cache was restored, skipping cleanup");
		await server.stop(pid);
		await save();
		return;
	}

	// get all paths that are in cache but not in local
	const pathsToRemove = cachePaths.filter((p) => !localPaths.includes(p));
	core.info(`found ${pathsToRemove.length} old paths to remove from cache`);

	// remove paths from cache
	for (const path of pathsToRemove) {
		core.info(`removing ${path} from cache`);
		const info = await nix.store.info(`file://${cachePath}`, path);

		await io.rmRF(`${cachePath}/${info.narInfo}`);
		await io.rmRF(`${cachePath}/${info.url}`);
	}

	await save();
	await server.stop(pid);
}

// check if path exists in substituter
export async function check(path: string, substituter: string) {
	substituter = substituter.replace(/\/+$/, ""); // remove trailing slash
	const narInfo = `${getTextBetween(path, "/nix/store/", "-")}.narinfo`;
	const res = await request(`${substituter}/${narInfo}`, { method: "HEAD" });

	return res.statusCode < 300;
}

// save to action cache
async function save() {
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
		[cachePath, keyPath],
		`nix-cache-${flakeHash}-${lockHash}-${github.context.workflow}-${github.context.job}`,
	);
}

try {
	await main();
} catch (error) {
	if (error instanceof Error) core.setFailed(error.message);
}

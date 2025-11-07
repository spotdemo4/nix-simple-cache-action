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
		core.warning("Cache is not available");
		return;
	}

	// get proxy server pid to make sure it's running
	const pidStr = core.getState("pid");
	if (!pidStr) {
		core.warning("No proxy server running");
		return;
	}
	const pid = parseInt(pidStr, 10);

	// get hit type
	const hitType = core.getState("hit-type");
	if (hitType === "direct") {
		core.info("Cache was a direct hit, skipping save");
		await server.stop(pid);
		return;
	}

	// optimise nix store
	await nix.store.optimise();

	// get nix store paths in local and cache
	const localPaths = (await nix.store.list()).filter(
		(p) => !p.endsWith(".drv"),
	);
	const cachePaths = (await nix.store.list(`file://${cachePath}`)).filter(
		(p) => !p.endsWith(".drv"),
	);

	// get all substituters
	const substituters = await nix.substituters();
	core.info(`Substituters: ${substituters.join(", ")}`);

	// check all paths in parallel
	let pathsToCheck = localPaths.filter((p) => !cachePaths.includes(p));
	let pathsToCopy: string[] = [];
	while (pathsToCheck.length > 0) {
		core.info(`Checking ${pathsToCheck.length} paths against substituters`);
		const checked = await checkAll(pathsToCheck, substituters);
		pathsToCopy = pathsToCopy.concat(
			checked
				.filter((p) => p.state === PatchCheckState.Uncached)
				.map((p) => p.path),
		);
		pathsToCheck = checked
			.filter((p) => p.state === PatchCheckState.Failed)
			.map((p) => p.path);
	}

	// copy paths to cache
	core.startGroup(`Copying ${pathsToCopy.length} paths to cache`);
	for (const path of pathsToCopy) {
		core.info(path);
		await nix.store.sign(path);
		await nix.store.copy(path, `file://${cachePath}`);
	}
	core.endGroup();

	if (hitType === "none") {
		core.info("No cache was restored, skipping cleanup");
		await server.stop(pid);
		await save();
		return;
	}

	// get all paths that are in cache but not in local
	const pathsToRemove = cachePaths.filter((p) => !localPaths.includes(p));

	// remove paths from cache
	core.startGroup(`Removing ${pathsToRemove.length} old paths from cache`);
	for (const path of pathsToRemove) {
		core.info(path);
		const info = await nix.store.info(`file://${cachePath}`, path);

		await io.rmRF(`${cachePath}/${info.narInfo}`);
		await io.rmRF(`${cachePath}/${info.url}`);
	}
	core.endGroup();

	await save();
	await server.stop(pid);
}

enum PatchCheckState {
	Uncached = 1,
	Cached,
	Failed,
}

type PathCheckResult = {
	path: string;
	state: PatchCheckState;
};

async function checkAll(paths: string[], substituters: string[]) {
	return await Promise.all(
		paths.map(async (path) => {
			const pathCheck: PathCheckResult = {
				path,
				state: PatchCheckState.Uncached,
			};

			for (const sub of substituters) {
				try {
					const c = await check(path, sub);
					if (c) {
						pathCheck.state = PatchCheckState.Cached;
						break;
					}
				} catch {
					pathCheck.state = PatchCheckState.Failed;
					break;
				}
			}

			return pathCheck;
		}),
	);
}

// check if path exists in substituter
export async function check(path: string, substituter: string) {
	const narInfo = `${getTextBetween(path, "/nix/store/", "-")}.narinfo`;
	const res = await request(`${substituter}/${narInfo}`, {
		method: "HEAD",
		reset: true,
		bodyTimeout: 0,
	});

	return res.statusCode < 300;
}

// save to action cache
async function save() {
	// get flake hash from state
	const flakeHash = core.getState("flake-hash");
	if (!flakeHash) {
		core.warning("Flake hash not found, not saving cache");
		return;
	}

	// get lock hash from state
	const lockHash = core.getState("lock-hash");
	if (!lockHash) {
		core.warning("Lock hash not found, not saving cache");
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

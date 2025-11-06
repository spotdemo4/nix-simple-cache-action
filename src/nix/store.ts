import * as fs from "node:fs";
import * as exec from "@actions/exec";
import { getTextBetween } from "../util.js";
import { keyPath } from "../var.js";

// optimise the nix store
export async function optimise() {
	await exec.exec("nix", ["store", "optimise"], {
		silent: true,
	});
}

// sign a specific path in the store
export async function sign(path: string) {
	await exec.exec("nix", ["store", "sign", "--key-file", keyPath, path], {});
}

// ping store to check if reachable
export async function ping(store: string) {
	const e = await exec.exec("nix", ["store", "info", "--store", store], {
		ignoreReturnCode: true,
	});

	return e === 0;
}

// list all paths in store
export async function list(store?: string) {
	if (store?.startsWith("file://")) {
		// check if path exists
		const path = store.replace("file://", "");
		if (!fs.existsSync(path)) {
			return [];
		}
	}

	const e = await exec.getExecOutput(
		"nix",
		["path-info", "--all", ...(store ? ["--store", store] : [])],
		{
			silent: true,
		},
	);

	return e.stdout.trim().split("\n");
}

// check if path exists in store
export async function check(store: string, path: string) {
	const e = await exec.exec(
		"nix",
		["path-info", "--recursive", "--store", store, path],
		{ ignoreReturnCode: true, silent: true },
	);

	return e === 0;
}

// copy path to store
export async function copy(path: string, store: string) {
	await exec.exec("nix", ["copy", "--to", store, path], {});
}

export interface Path {
	compression: string;
	deriver: string;
	downloadHash: string;
	downloadSize: number;
	narHash: string;
	narInfo: string;
	narSize: number;
	references: string[];
	signatures: string[];
	ultimate: boolean;
	url: string;
}

// get info about path in store
export async function info(store: string, path: string) {
	const e = await exec.getExecOutput(
		"nix",
		["path-info", "--json", "--store", store, path],
		{
			silent: true,
		},
	);

	const pathInfo = Object.values(JSON.parse(e.stdout))[0] as Path;
	pathInfo.narInfo = `${getTextBetween(path, "/nix/store/", "-")}.narinfo`;

	return pathInfo;
}

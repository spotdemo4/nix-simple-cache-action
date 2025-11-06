import { writeFileSync } from "node:fs";
import * as exec from "@actions/exec";
import { keyName, keyPath } from "../var.js";

export * as store from "./store.js";

export async function version() {
	const e = await exec.getExecOutput("nix", ["--version"], {
		silent: true,
	});

	return e.stdout.trim();
}

export async function hash(filename: string) {
	const e = await exec.getExecOutput(
		"nix",
		["hash", "file", "--type", "sha1", "--base64", filename],
		{
			silent: true,
		},
	);

	return e.stdout.trim();
}

export async function substituters() {
	const configSubs = await getConfigSubstituters();
	const flakeSubs = await getFlakeSubstituters();
	let subs = [...new Set([...configSubs, ...flakeSubs])];
	subs = subs.map((s) => s.trim().replace(/\/+$/, ""));

	return subs;
}

async function getConfigSubstituters() {
	const e = await exec.getExecOutput(
		"nix",
		["config", "show", "substituters"],
		{
			silent: true,
		},
	);

	return e.stdout
		.split(" ")
		.map((s) => s.trim())
		.filter((s) => !s.includes("127.0.0.1"));
}

async function getFlakeSubstituters() {
	const f = await exec.getExecOutput(
		"nix",
		["eval", "--json", "--file", "./flake.nix", "nixConfig"],
		{
			silent: true,
			ignoreReturnCode: true,
		},
	);
	if (f.exitCode !== 0) {
		return [];
	}

	try {
		const parsed = JSON.parse(f.stdout);
		const flakeSubs: string[] = parsed.substituters || [];
		const flakeExtraSubs: string[] = parsed["extra-substituters"] || [];

		return [...new Set([...flakeSubs, ...flakeExtraSubs])];
	} catch {
		return [];
	}
}

export async function generateSecretKey() {
	const e = await exec.getExecOutput(
		"nix",
		["key", "generate-secret", "--key-name", keyName],
		{
			silent: true,
		},
	);
	const secretKey = e.stdout.trim();

	writeFileSync(keyPath, secretKey);
}

export async function getPublicKey() {
	const e = await exec.getExecOutput(
		"bash",
		["-c", `cat ${keyPath} | nix key convert-secret-to-public`],
		{
			silent: true,
		},
	);

	return e.stdout.trim();
}

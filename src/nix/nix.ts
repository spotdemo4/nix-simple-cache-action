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
	const e = await exec.getExecOutput(
		"nix",
		["config", "show", "substituters"],
		{
			silent: true,
		},
	);
	const configSubs = e.stdout
		.split(" ")
		.map((s) => s.trim())
		.filter((s) => !s.includes("127.0.0.1"));

	const f = await exec.getExecOutput(
		"nix",
		["eval", "--json", "--file", "./flake.nix", "nixConfig"],
		{
			silent: true,
		},
	);
	const parsed = JSON.parse(f.stdout);
	const flakeSubs: string[] = parsed.substituters || [];
	const flakeExtraSubs: string[] = parsed["extra-substituters"] || [];

	let subs = [...new Set([...configSubs, ...flakeSubs, ...flakeExtraSubs])];
	subs = subs.map((s) => s.trim().replace(/\/+$/, ""));
	return subs;
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

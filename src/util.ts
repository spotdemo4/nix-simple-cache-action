import { spawn } from "node:child_process";
import { existsSync, openSync, readFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import * as core from "@actions/core";
import * as exec from "@actions/exec";
import { request } from "undici";

// start proxy server
export async function startServer(port: string, root: string) {
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
	const out = openSync(`/tmp/out-${port}.log`, "as"); // Open file for stdout
	const err = openSync(`/tmp/err-${port}.log`, "as"); // Open file for stderr
	const proxy = spawn("node", [`${__dirname}/proxy.js`, port, root], {
		detached: true,
		stdio: ["ignore", out, err],
	});
	proxy.unref();

	// wait for the proxy server to start
	let ping = 1;
	let attempts = 0;
	while (ping !== 0 && attempts < 5) {
		ping = await exec.exec(
			"nix",
			["store", "info", "--store", `http://127.0.0.1:${port}`],
			{ ignoreReturnCode: true, silent: true },
		);

		if (ping !== 0) {
			attempts++;
			core.info(`waiting for proxy server to start, attempt ${attempts}...`);
			await new Promise((resolve) => setTimeout(resolve, 1000));
		}
	}

	// starting failed
	if (attempts >= 5) {
		core.warning("proxy server did not start.");
		core.warning(`stdout: ${readFileSync(`/tmp/out-${port}.log`, "utf8")}`);
		core.warning(`stderr: ${readFileSync(`/tmp/err-${port}.log`, "utf8")}`);
		return;
	}

	// return pid
	return proxy.pid;
}

// close proxy server
export async function stopServer(pid: number, port: string) {
	process.kill(pid);

	// print proxy stdout to debug
	const stdout = readFileSync(`/tmp/out-${port}.log`, "utf8").trim();
	if (stdout) {
		core.debug("proxy server stdout:");
		core.debug(stdout);
	}

	// print proxy errors if they exist
	const stderr = readFileSync(`/tmp/err-${port}.log`, "utf8").trim();
	if (stderr) {
		core.warning("proxy server exited with errors");
		core.info("proxy server stderr:");
		core.info(stderr);
	}
}

// have proxy server load in substituters so already cached paths are not added
export async function loadSubstituters(port: string) {
	core.info("loading substituters");
	const { statusCode, body } = await request(
		`http://127.0.0.1:${port}/substituters`,
		{
			method: "POST",
		},
	);
	if (statusCode >= 300) {
		core.warning("failed to load substituters");
		return;
	}

	const substituters = (await body.json()) as string[];
	core.info(`substituters: ${substituters.join(", ")}`);
}

import { spawn } from "node:child_process";
import { existsSync, openSync, readFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import * as core from "@actions/core";
import * as nix from "../nix/nix.js";
import { errPath, logPath, port } from "../var.js";

// create and start a nix store HTTP binary proxy server
export async function start() {
	// check if already running
	if (core.getState("pid")) {
		core.warning(`Proxy server already running`);
		return false;
	}

	// determine path to server.js
	const __filename = fileURLToPath(import.meta.url);
	const __dirname = dirname(__filename);
	if (!existsSync(`${__dirname}/server.js`)) {
		core.warning(`File ${__dirname}/server.js not found`);
		return false;
	}

	// start server
	const out = openSync(logPath, "as"); // Open file for stdout
	const err = openSync(errPath, "as"); // Open file for stderr
	const proxy = spawn("node", [`${__dirname}/server.js`, port], {
		detached: true,
		stdio: ["ignore", out, err],
	});
	proxy.unref();

	// wait for the proxy server to start
	let ping = false;
	let attempts = 0;
	while (!ping && attempts < 5) {
		ping = await nix.store.ping(`http://127.0.0.1:${port}`);

		if (!ping) {
			attempts++;
			core.info(`Waiting for proxy server to start, attempt ${attempts}`);
			await new Promise((resolve) => setTimeout(resolve, 1000));
		}
	}

	// starting failed
	if (attempts >= 5 || !proxy.pid) {
		core.warning("Proxy server did not start.");
		core.warning(`Stdout: ${readFileSync(logPath, "utf8")}`);
		core.warning(`Stderr: ${readFileSync(errPath, "utf8")}`);
		return false;
	}

	// set pid
	core.saveState("pid", proxy.pid.toString());

	return true;
}

// close proxy server
export async function stop(pid: number) {
	// kill process
	process.kill(pid);

	// print proxy stdout to debug
	const stdout = readFileSync(logPath, "utf8").trim();
	if (stdout) {
		core.debug("Proxy server stdout:");
		core.debug(stdout);
	}

	// print proxy errors if they exist
	const stderr = readFileSync(errPath, "utf8").trim();
	if (stderr) {
		core.warning("Proxy server exited with errors");
		core.info("Proxy server stderr:");
		core.info(stderr);
	}
}

import { spawn } from "node:child_process";
import { existsSync, openSync, readFileSync } from "node:fs";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import * as core from "@actions/core";
import * as nix from "../nix/nix.js";
import { errPath, logPath } from "../var.js";

// create and start a nix store HTTP binary proxy server
export async function start() {
	// check if already running
	if (core.getState("pid")) {
		core.warning(`proxy server already running`);
		return false;
	}

	// determine path to server.js
	const __filename = fileURLToPath(import.meta.url);
	const __dirname = dirname(__filename);
	if (!existsSync(`${__dirname}/server.js`)) {
		core.warning(
			`${__dirname}/server.js not found, skipping binary cache server`,
		);
		return false;
	}

	// get port for server to run on
	const port = core.getInput("port") || "5001";

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
		ping = await nix.store.ping(port);

		if (!ping) {
			attempts++;
			core.info(`waiting for proxy server to start, attempt ${attempts}...`);
			await new Promise((resolve) => setTimeout(resolve, 1000));
		}
	}

	// starting failed
	if (attempts >= 5 || !proxy.pid) {
		core.warning("proxy server did not start.");
		core.warning(`stdout: ${readFileSync(logPath, "utf8")}`);
		core.warning(`stderr: ${readFileSync(errPath, "utf8")}`);
		return false;
	}

	// set pid
	core.saveState("pid", proxy.pid.toString());

	return true;
}

// close proxy server
export async function stop() {
	const pidStr = core.getState("pid");
	if (!pidStr) {
		core.warning("no proxy server running");
		return;
	}

	// kill process
	const pid = parseInt(pidStr, 10);
	process.kill(pid);

	// print proxy stdout to debug
	const stdout = readFileSync(logPath, "utf8").trim();
	if (stdout) {
		core.debug("proxy server stdout:");
		core.debug(stdout);
	}

	// print proxy errors if they exist
	const stderr = readFileSync(errPath, "utf8").trim();
	if (stderr) {
		core.warning("proxy server exited with errors");
		core.info("proxy server stderr:");
		core.info(stderr);
	}
}

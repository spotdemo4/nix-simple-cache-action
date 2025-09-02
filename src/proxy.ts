import { exec } from "node:child_process";
import {
	createReadStream,
	createWriteStream,
	existsSync,
	mkdirSync,
} from "node:fs";
import { createServer } from "node:http";
import path from "node:path";
import { promisify } from "node:util";
import { request } from "undici";

const execPromise = promisify(exec);

const root = "/tmp/nix-cache";
const hostname = "127.0.0.1";
const port = 5001;
const mimeTypes: Record<string, string> = {
	".nar": "application/x-nix-nar",
	".nar.xz": "application/x-xz",
	".nar.zst": "application/zstd",
	".narinfo": "application/x-nix-narinfo",
};
let substituters: string[] = [];

const server = createServer(async (req, res) => {
	try {
		if (!req.url) return;

		switch (req.method) {
			case "HEAD": {
				// check if requested path exists locally
				const localPath = path.join(root, req.url);
				if (existsSync(localPath)) {
					console.log("✓", localPath);

					// return good status code
					res.writeHead(200);
					res.end();

					return;
				}

				// also check if any substituter has the requested path
				for (const substituter of substituters) {
					const substituterURL = new URL(req.url, substituter);

					// proxy the request
					delete req.headers.host;
					delete req.headers.referer;
					const { statusCode, headers } = await request(substituterURL, {
						method: "HEAD",
						headers: req.headers,
					});
					if (statusCode >= 300) continue;

					console.log("✓", substituterURL.href);

					// return status
					res.writeHead(statusCode, headers);
					res.end();

					return;
				}

				// else not found
				console.log("x", req.url);
				res.writeHead(404, { "Content-Type": "text/plain" });
				res.end("not found");

				return;
			}

			case "GET": {
				// check if requested path exists locally
				const localPath = path.join(root, req.url);
				if (existsSync(localPath)) {
					console.log("<-", localPath);

					// determine content type
					const ext = path.parse(localPath).ext;
					const contentType = mimeTypes[ext] || "application/octet-stream";
					res.writeHead(200, {
						"Content-Type": contentType,
						"Content-Disposition": `attachment; filename="${path.basename(localPath)}"`,
					});

					// pipe the file to response
					const fileStream = createReadStream(localPath);
					fileStream.on("error", (err) => {
						console.error("error streaming file:", err);
						res.end("error streaming file");
					});
					fileStream.pipe(res);

					return;
				}

				// also check if any substituter has the requested path
				for (const substituter of substituters) {
					const substituterURL = new URL(req.url, substituter);

					// proxy the request
					delete req.headers.host;
					delete req.headers.referer;
					const { statusCode, headers, body } = await request(substituterURL, {
						method: "GET",
						headers: req.headers,
					});
					if (statusCode >= 300) continue;

					console.log("<-", substituterURL.href);

					// pipe store to response
					res.writeHead(statusCode, headers);
					body.pipe(res, {
						end: true,
					});

					return;
				}

				// else not found
				console.log("x", req.url);
				res.writeHead(404, { "Content-Type": "text/plain" });
				res.end("not found");

				return;
			}

			case "PUT": {
				// ensure directory exists
				const localPath = path.join(root, req.url);
				const dir = path.dirname(localPath);
				if (!existsSync(dir)) {
					mkdirSync(dir, { recursive: true });
				}

				console.log("->", localPath);

				// create write stream
				const fileStream = createWriteStream(localPath, {
					flags: "w+",
					encoding: "binary",
				});

				// pipe request to file
				fileStream.on("finish", () => {
					res.writeHead(201, { "Content-Type": "text/plain" });
					res.end("created");
				});
				req.pipe(fileStream);

				return;
			}

			case "POST": {
				if (req.url !== "/substituters") {
					res.writeHead(404, { "Content-Type": "text/plain" });
					res.end("not found");
					return;
				}

				// update substitutors
				substituters = (
					await execPromise("nix config show substituters")
				).stdout
					.split(" ")
					.map((s) => s.trim());
				console.log("substituters:", substituters.join(", "));
				res.writeHead(200, { "Content-Type": "application/json" });
				res.end(JSON.stringify(substituters));

				return;
			}
		}
	} catch (err) {
		console.error("error handling request:", err);
		res.writeHead(500, { "Content-Type": "text/plain" });
		res.end("internal server error");
	}
});

console.log(`starting server at http://${hostname}:${port}`);
server.listen(port, hostname);

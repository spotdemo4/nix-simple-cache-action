import {
	createReadStream,
	createWriteStream,
	existsSync,
	mkdirSync,
} from "node:fs";
import { createServer } from "node:http";
import path from "node:path";
import { cachePath, port } from "../var.js";

const hostname = "127.0.0.1";
const mimeTypes: Record<string, string> = {
	".nar": "application/x-nix-nar",
	".nar.xz": "application/x-xz",
	".nar.zst": "application/zstd",
	".narinfo": "application/x-nix-narinfo",
};

const server = createServer(async (req, res) => {
	try {
		if (!req.url) return;

		switch (req.method) {
			case "HEAD": {
				// check if requested path exists locally
				const localPath = path.join(cachePath, req.url);
				if (!existsSync(localPath)) {
					console.log("x", req.url);
					res.writeHead(404, { "Content-Type": "text/plain" });
					res.end("not found");
					return;
				}

				console.log("✓", localPath);

				// return good status code
				res.writeHead(200);
				res.end();

				return;
			}

			case "GET": {
				// check if requested path exists locally
				const localPath = path.join(cachePath, req.url);
				if (!existsSync(localPath)) {
					console.log("x", req.url);
					res.writeHead(404, { "Content-Type": "text/plain" });
					res.end("not found");
					return;
				}

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

			case "PUT": {
				// ensure directory exists
				const localPath = path.join(cachePath, req.url);
				const dir = path.dirname(localPath);
				if (!existsSync(dir)) {
					mkdirSync(dir, { recursive: true });
					return;
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
		}
	} catch (err) {
		console.error("error handling request:", err);
		res.writeHead(500, { "Content-Type": "text/plain" });
		res.end("internal server error");
	}
});

console.log(`starting server at http://${hostname}:${port}`);
server.listen(parseInt(port, 10), hostname);

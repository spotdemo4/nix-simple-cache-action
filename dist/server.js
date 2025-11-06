import { createReadStream, createWriteStream, existsSync, mkdirSync } from "node:fs";
import { createServer } from "node:http";
import path from "node:path";

//#region src/var.ts
const cachePath = "/tmp/nix-cache";

//#endregion
//#region src/server/server.ts
const hostname = "127.0.0.1";
const port = process.argv[2] ? parseInt(process.argv[2], 10) : 5001;
const mimeTypes = {
	".nar": "application/x-nix-nar",
	".nar.xz": "application/x-xz",
	".nar.zst": "application/zstd",
	".narinfo": "application/x-nix-narinfo"
};
const server = createServer(async (req, res) => {
	try {
		if (!req.url) return;
		switch (req.method) {
			case "HEAD": {
				const localPath = path.join(cachePath, req.url);
				if (!existsSync(localPath)) {
					console.log("x", req.url);
					res.writeHead(404, { "Content-Type": "text/plain" });
					res.end("not found");
					return;
				}
				console.log("✓", localPath);
				res.writeHead(200);
				res.end();
				return;
			}
			case "GET": {
				const localPath = path.join(cachePath, req.url);
				if (!existsSync(localPath)) {
					console.log("x", req.url);
					res.writeHead(404, { "Content-Type": "text/plain" });
					res.end("not found");
					return;
				}
				console.log("<-", localPath);
				const contentType = mimeTypes[path.parse(localPath).ext] || "application/octet-stream";
				res.writeHead(200, {
					"Content-Type": contentType,
					"Content-Disposition": `attachment; filename="${path.basename(localPath)}"`
				});
				const fileStream = createReadStream(localPath);
				fileStream.on("error", (err) => {
					console.error("error streaming file:", err);
					res.end("error streaming file");
				});
				fileStream.pipe(res);
				return;
			}
			case "PUT": {
				const localPath = path.join(cachePath, req.url);
				const dir = path.dirname(localPath);
				if (!existsSync(dir)) {
					mkdirSync(dir, { recursive: true });
					return;
				}
				console.log("->", localPath);
				const fileStream = createWriteStream(localPath, {
					flags: "w+",
					encoding: "binary"
				});
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
server.listen(port, hostname);

//#endregion
export {  };
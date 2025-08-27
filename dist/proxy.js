import { exec } from 'node:child_process';
import { existsSync, mkdirSync, createWriteStream, createReadStream } from 'node:fs';
import { createServer } from 'node:http';
import path from 'node:path';
import { promisify } from 'node:util';
import { request } from 'node:https';

function requestPromise(options, secure) {
    return new Promise((resolve) => {
        const request$1 = request ;
        const req = request$1(options, (res) => {
            resolve(res);
        });
        // catch timeout
        req.setTimeout(300000, () => {
            console.error(`request "${options.path}" timed out`);
            req.destroy(); // destroy the request if a timeout occurs
            resolve(null);
        });
        // catch error
        req.on("error", (err) => {
            console.error(`request "${options.path}" error: ${err.message}`);
            resolve(null);
        });
        req.end();
    });
}

const execPromise = promisify(exec);
const root = "/tmp/nix-cache";
const hostname = "127.0.0.1";
const port = 5001;
const mimeTypes = {
    ".nar": "application/x-nix-nar",
    ".nar.xz": "application/x-xz",
    ".nar.zst": "application/zstd",
    ".narinfo": "application/x-nix-narinfo",
};
let substituters = [];
const server = createServer(async (req, res) => {
    try {
        if (!req.url)
            return;
        switch (req.method) {
            case "HEAD": {
                // check if any substituter has the requested path
                for (const substituter of substituters) {
                    const substituterURL = new URL(req.url, substituter);
                    delete req.headers.host;
                    delete req.headers.referer;
                    const head = await requestPromise({
                        hostname: substituterURL.hostname,
                        port: 443,
                        path: req.url,
                        method: req.method,
                        headers: req.headers,
                    }, true);
                    if (!head || !head.statusCode || head.statusCode >= 300)
                        continue;
                    console.log("✓", substituterURL.href);
                    // return status
                    res.writeHead(head.statusCode, head.headers);
                    res.end();
                    return;
                }
                // else check if requested path exists locally
                const localPath = path.join(root, req.url);
                if (!existsSync(localPath)) {
                    console.log("x", localPath);
                    res.writeHead(404, { "Content-Type": "text/plain" });
                    res.end("not found");
                    return;
                }
                console.log("✓", localPath);
                // return good status code
                res.writeHead(200);
                res.end();
                break;
            }
            case "GET": {
                // check if any substituter has the requested path
                for (const substituter of substituters) {
                    const substituterURL = new URL(req.url, substituter);
                    delete req.headers.host;
                    delete req.headers.referer;
                    const get = await requestPromise({
                        hostname: substituterURL.hostname,
                        port: 443,
                        path: req.url,
                        method: req.method,
                        headers: req.headers,
                    }, true);
                    if (!get || !get.statusCode || get.statusCode >= 300)
                        continue;
                    console.log("<-", substituterURL.href);
                    // pipe store path to response
                    res.writeHead(get.statusCode, get.headers);
                    get.pipe(res, {
                        end: true,
                    });
                    return;
                }
                // else check if requested path exists locally
                const localPath = path.join(root, req.url);
                if (!existsSync(localPath)) {
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
                break;
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
                break;
            }
            case "POST": {
                if (req.url !== "/substituters") {
                    res.writeHead(404, { "Content-Type": "text/plain" });
                    res.end("not found");
                    return;
                }
                // update substitutors
                substituters = (await execPromise("nix config show substituters")).stdout
                    .split(" ")
                    .map((s) => s.trim());
                console.log("substituters:", substituters.join(", "));
                res.writeHead(200, { "Content-Type": "text/plain" });
                res.end(JSON.stringify(substituters));
                break;
            }
        }
    }
    catch (err) {
        console.error("error handling request:", err);
        res.writeHead(500, { "Content-Type": "text/plain" });
        res.end("internal server error");
    }
});
console.log(`starting server at http://${hostname}:${port}`);
server.listen(port, hostname);
//# sourceMappingURL=proxy.js.map

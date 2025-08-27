import { request as http, type IncomingMessage } from "node:http";
import { request as https, type RequestOptions } from "node:https";

export function requestPromise(
	options: RequestOptions | string | URL,
	secure?: boolean,
): Promise<IncomingMessage> {
	return new Promise((resolve, reject) => {
		const request = secure ? https : http;

		const req = request(options, (res) => {
			resolve(res);
		});

		req.setTimeout(300000, () => {
			req.destroy(); // destroy the request if a timeout occurs
			reject(new Error("request timed out"));
		});
		req.on("error", (err) => {
			reject(err);
		});
		req.end();
	});
}

export function streamToString(stream: NodeJS.ReadableStream): Promise<string> {
	const chunks: Buffer[] = [];
	return new Promise((resolve, reject) => {
		stream.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
		stream.on("error", (err) => reject(err));
		stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
	});
}

export function formatBytes(bytes: number, decimals = 2) {
	if (!+bytes) return "0 Bytes";

	const k = 1024;
	const dm = decimals < 0 ? 0 : decimals;
	const sizes = [
		"Bytes",
		"KiB",
		"MiB",
		"GiB",
		"TiB",
		"PiB",
		"EiB",
		"ZiB",
		"YiB",
	];

	const i = Math.floor(Math.log(bytes) / Math.log(k));

	return `${parseFloat((bytes / k ** i).toFixed(dm))} ${sizes[i]}`;
}

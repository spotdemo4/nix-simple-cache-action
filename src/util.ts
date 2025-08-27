import { request as http, type IncomingMessage } from "node:http";
import { request as https, type RequestOptions } from "node:https";

export function requestPromise(
	options: RequestOptions,
	secure?: boolean,
): Promise<IncomingMessage | null> {
	return new Promise((resolve) => {
		const request = secure ? https : http;

		const req = request(options, (res) => {
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

export function streamToString(
	stream: NodeJS.ReadableStream,
): Promise<string | null> {
	const chunks: Buffer[] = [];
	return new Promise((resolve) => {
		stream.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
		stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
		stream.on("error", (err) => {
			console.error(`error reading stream: ${err}`);
			resolve(null);
		});
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

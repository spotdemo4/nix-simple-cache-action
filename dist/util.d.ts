import { type IncomingMessage } from "node:http";
import { type RequestOptions } from "node:https";
export declare function requestPromise(options: RequestOptions, secure?: boolean): Promise<IncomingMessage | null>;
export declare function streamToString(stream: NodeJS.ReadableStream): Promise<string | null>;
export declare function formatBytes(bytes: number, decimals?: number): string;
//# sourceMappingURL=util.d.ts.map
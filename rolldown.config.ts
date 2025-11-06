import { defineConfig } from "rolldown";

export default defineConfig([
	{
		input: "src/start.ts",
		output: {
			file: "dist/start.js",
		},
		platform: "node",
	},
	{
		input: "src/cleanup.ts",
		output: {
			file: "dist/cleanup.js",
		},
		platform: "node",
	},
	{
		input: "src/server/server.ts",
		output: {
			file: "dist/server.js",
		},
		platform: "node",
	},
]);

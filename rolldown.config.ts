import { defineConfig } from "rolldown";

export default defineConfig([
	{
		input: "src/index.ts",
		output: {
			file: "dist/index.js",
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
		input: "src/proxy.ts",
		output: {
			file: "dist/proxy.js",
		},
		platform: "node",
	},
]);

import { defineConfig } from "oxlint";

export default defineConfig({
  ignorePatterns: ["dist", "src/proto/**", "src/routeTree.gen.ts"],
});

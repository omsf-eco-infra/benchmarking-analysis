import { defineConfig } from "astro/config";

export default defineConfig({
  // Keep the prototype separate from the deployed marimo export.
  outDir: "../output-astro",
  build: { format: "directory" },
  trailingSlash: "always",
  devToolbar: { enabled: false },
});

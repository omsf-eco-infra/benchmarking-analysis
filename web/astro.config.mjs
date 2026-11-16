import { defineConfig } from "astro/config";

export default defineConfig({
  // Keep Astro output separate from the optional marimo export.
  outDir: "../output-astro",
  build: { format: "directory" },
  trailingSlash: "always",
  devToolbar: { enabled: false },
});

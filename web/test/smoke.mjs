import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, extname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { formatInteger, formatNumber, formatTick } from "../src/lib/format.ts";

const web = dirname(dirname(fileURLToPath(import.meta.url)));
const output = join(web, "..", "output-astro");
const manifest = JSON.parse(readFileSync(join(web, "src/generated/manifest.json"), "utf8"));
const read = (file) => readFileSync(file, "utf8");
const escape = (value) =>
  String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");

function filesIn(dir) {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const path = join(dir, entry.name);
    return entry.isDirectory() ? filesIn(path) : [path];
  });
}

// --- formatting helpers ---
assert.equal(formatNumber(null), "—");
assert.equal(formatNumber(Infinity), "—");
assert.equal(formatNumber(1234.567), "1,234.57");
assert.equal(formatInteger(null), "—");
assert.equal(formatInteger(49052), "49,052");
assert.equal(formatTick(1234.5), "1,235");
assert.equal(formatTick(12.34), "12.3");
assert.equal(formatTick(2.678), "2.68");

// --- manifest contract ---
assert(manifest.views.length > 0);
assert.equal(new Set(manifest.views.map((view) => view.path)).size, manifest.views.length);
for (const view of manifest.views) {
  assert(view.charts.length > 0, `${view.path}: charts missing`);
  for (const chart of view.charts) {
    assert(
      ["ns_per_day", "ns_per_dollar", "dollars_per_edge"].includes(chart.metric),
      `${view.path}: bad metric ${chart.metric}`,
    );
    assert.equal(typeof chart.label, "string", `${view.path}: chart label must be a string`);
    assert(!("src" in chart), `${view.path}: charts must not reference an SVG`);
    assert.equal(Object.keys(chart).sort().join(","), "label,metric");
  }
}
for (const source of manifest.sources) assert.match(source.sha256, /^[0-9a-f]{64}$/);

// --- one page plus a harmless 404, no static charts ---
const files = filesIn(output);
assert(!files.some((file) => /[/\\]charts[/\\]/.test(file)), "Chart SVGs must not be emitted");
assert.equal(
  files.filter((file) => file.endsWith(".svg")).length,
  1,
  "Only the favicon should be an SVG",
);
const pages = files.filter((file) => file.endsWith(".html"));
assert.deepEqual(
  pages.map((page) => page.slice(output.length)).sort(),
  ["/404.html", "/index.html"],
  "Exactly index and 404 should be built",
);

const index = read(join(output, "index.html"));
const notFound = read(join(output, "404.html"));

// Astro can trim the newline between prose and an inline tag, joining words.
for (const [name, html] of [
  ["index.html", index],
  ["404.html", notFound],
]) {
  const body = html.replace(/<script[\s\S]*?<\/script>/g, "");
  for (const match of body.matchAll(/[A-Za-z0-9]<(code|strong|em|a|span)\b/g)) {
    assert.fail(`${name}: joined text before <${match[1]}>`);
  }
  for (const match of body.matchAll(/<\/(code|strong|em|a|span)>[A-Za-z0-9]/g)) {
    assert.fail(`${name}: joined text after </${match[1]}>`);
  }
}

const viewsFor = (workload, kind) =>
  manifest.views
    .filter((view) => view.workload === workload && view.kind === kind)
    .sort((a, b) =>
      typeof a.selection === "number" && typeof b.selection === "number"
        ? a.selection - b.selection
        : String(a.selection).localeCompare(String(b.selection)),
    );

// --- four result sections in the required order ---
const expectedSections = [
  ["md-comparison", "md", "mps"],
  ["md-scaling", "md", "systems"],
  ["rbfe-comparison", "rbfe", "mps"],
  ["rbfe-scaling", "rbfe", "systems"],
];
const blocks = [
  ...index.matchAll(
    /<section class="result-section" id="([^"]+)" data-workload="([^"]+)" data-kind="([^"]+)"[^>]*>([\s\S]*?)(?=<section |<\/main>)/g,
  ),
];
assert.equal(blocks.length, 4, "Expected four result sections");
assert.deepEqual(
  blocks.map((block) => [block[1], block[2], block[3]]),
  expectedSections,
  "Sections out of order",
);

let chartCount = 0;
for (const [, id, workload, kind, inner] of blocks) {
  const views = viewsFor(workload, kind);
  assert(views.length > 1, `${id}: expected multiple selections`);
  const defaultView = views[0];

  // Dropdown, label and options.
  const select = inner.match(/<select[^>]*data-view-select[^>]*>([\s\S]*?)<\/select>/);
  assert(select, `${id}: dropdown missing`);
  assert(select[0].includes(" disabled"), `${id}: select should wait for the client renderer`);
  const options = [...select[1].matchAll(/<option value="([^"]+)"( selected)?>/g)];
  assert.deepEqual(
    options.map((option) => option[1]),
    views.map((view) => view.path),
    `${id}: dropdown options`,
  );
  assert.equal(options[0][2], " selected", `${id}: first option must be selected by default`);
  assert(inner.includes(`<label for="${id}-select">`), `${id}: select label missing`);
  assert(inner.includes(escape(defaultView.title)), `${id}: default view title missing`);

  // Charts: one live canvas per metric, no static images.
  const metrics = [...inner.matchAll(/data-metric="([^"]+)"/g)]
    .map((match) => match[1])
    .sort();
  assert.deepEqual(metrics, defaultView.charts.map((chart) => chart.metric).sort());
  assert.equal(
    (inner.match(/data-chart-canvas/g) ?? []).length,
    defaultView.charts.length,
    `${id}: chart canvases`,
  );
  assert(!/<img\b/i.test(inner), `${id}: static chart image present`);
  chartCount += defaultView.charts.length;

  assert(!/<table\b/i.test(inner), `${id}: benchmark tables should not be shown`);

}

// --- notebook layout and build details ---
assert(!/<header\\b|<nav\\b|<footer\\b/.test(index), "Do not add dashboard chrome");
assert(index.includes("<h1>OMSF GPU Cloud Benchmark Analysis</h1>"));
assert(index.includes('class="instance-metadata"'), "Notebook GPU key missing");
const notebookHeadings = [
  "Molecular Dynamics (MD) benchmarks using OpenMM",
  "MPS scaling by MPS Process Count",
  "Relative Binding Free Energy (RBFE) benchmarks using Open Free Energy",
  "RBFE scaling by MPS process count",
  "About",
];
assert.deepEqual([...index.matchAll(/<h2[^>]*>([^<]+)<\/h2>/g)].map(match => match[1]), notebookHeadings);
for (const label of [
  "MPS process count (simultaneous simulations / GPU)", "MD system",
  "RBFE MPS process count", "RBFE system",
]) assert(index.includes(`>${label}</label>`), `Notebook control label missing: ${label}`);
for (const source of manifest.sources) {
  assert(index.includes(escape(source.file)), `Provenance missing ${source.file}`);
  assert(index.includes(source.sha256), `Provenance missing hash for ${source.file}`);
}
for (const term of ["median", "complex phase", "simultaneous processes per GPU"]) {
  assert(index.toLowerCase().includes(term.toLowerCase()), `Methodology missing "${term}"`);
}
assert(index.includes(escape(manifest.pricing_file)), "Pricing snapshot missing");
assert(index.includes('id="methodology"'), "Methodology anchor missing");
for (const [id] of expectedSections) {
  assert(index.includes(`id="${id}"`), `Missing section id ${id}`);
}

// --- no routes from the previous multi-page design ---
assert(!/href="\/(md|rbfe)\//.test(index), "Obsolete view route linked");
assert(!/href="\/methodology\/?"/.test(index), "Obsolete methodology route linked");
assert(!/href="\/(md|rbfe|methodology)\//.test(notFound), "Obsolete route on 404");

// --- bundled D3 dashboard ---
const script = index.match(/<script type="module" src="([^"]+\.js)">/);
assert(script, "Dashboard script tag missing");
const bundlePath = join(output, script[1].slice(1));
assert(existsSync(bundlePath), "Dashboard bundle missing");
const bundle = read(bundlePath);
assert(bundle.includes("omsf-d3-charts"), "D3 renderer marker missing from bundle");
assert(bundle.includes("http://www.w3.org/2000/svg"), "D3 not bundled");
assert(bundle.includes(manifest.views[0].path), "Manifest JSON not bundled");

// --- 404 page ---
assert(!/<script\b/i.test(notFound), "Client script on 404 page");
assert(notFound.includes('href="/"'), "404 should link back to the results page");

// --- every local link and fragment resolves ---
for (const page of pages) {
  const html = read(page);
  const base = new URL(page.slice(output.length).replace(/index\.html$/, ""), "https://static.test");
  for (const [, href] of html.matchAll(/(?:href|src)="([^"]+)"/g)) {
    const url = new URL(href, base);
    if (url.origin !== base.origin) continue;
    const target = join(output, url.pathname.slice(1), extname(url.pathname) ? "" : "index.html");
    assert(existsSync(target), `Broken link ${href} in ${page}`);
    if (url.hash && target.endsWith(".html")) {
      assert(
        read(target).includes(`id="${decodeURIComponent(url.hash.slice(1))}"`),
        `Broken anchor ${href} in ${page}`,
      );
    }
  }
}

console.log(
  `Single-page smoke test passed: 2 pages, 4 sections, ${chartCount} chart canvases, ` +
    `${manifest.views.length} dropdown selections, provenance, anchors and D3 bundle.`,
);

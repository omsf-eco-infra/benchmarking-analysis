# Astro single-page dashboard

Python precomputes every dropdown selection in `src/generated/manifest.json`.
Astro reproduces the notebook's introduction, GPU key, section headings, controls,
and About section. Comparison charts use one horizontal row of system panels
(120 × 260 px plot per system), with throughput and cost metrics stacked vertically.
MPS scaling uses 360 × 260 px plots. No dashboard header, navigation cards, or page navigation.
Pricing and provenance are collapsed under About. The only other page is a 404.

D3 draws live, responsive SVG bar charts from the bundled results. Dropdowns
update charts in place without navigation, reloads, APIs, or browser Python.
Backgrounds, axes, labels, and muted bars follow the active light/dark theme.
Hover highlights a bar and shows its exact value inline. Tooltips and SVG descriptions
provide full context and winner status. Themed surface gradients, accents, and subtle
transitions add polish without changing the chart geometry; reduced motion is respected.
Comparison facets share a y scale and scroll horizontally on narrow screens.
There are no benchmark result tables; the introduction retains the notebook's GPU key.

From the repository root (pixi and Node.js 24 required):

```bash
npm --prefix web ci
pixi run build-astro
npm --prefix web test
npm --prefix web run preview
```

Output: `../output-astro/`, separate from the existing notebook deployment.
Generated inputs are ignored by Git. For frontend development, export once and
run `npm --prefix web run dev`. JavaScript is needed to render the charts.

- `src/pages/index.astro`: dashboard, methodology, provenance.
- `src/components/Section.astro`: selector and chart panels.
- `src/lib/charts.ts`: D3 facets, grouped bars, tooltips, SVG descriptions.
- `src/scripts/dashboard.ts`: selection, resize, and theme updates.
- `test/smoke.mjs`: built page, controls, provenance, links, and D3 bundle.
- `../tests/browser_dashboard.py`: all 22 selections, bar data, shared scales,
  notebook panel layout, mobile scrolling, theme changes, and runtime errors.

`npm test` checks existing build output. Pricing overrides and browser-test setup
are documented in the root README. No server adapter or Worker runtime is needed.

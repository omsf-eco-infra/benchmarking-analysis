# Benchmarking Analysis

Interactive analysis dashboard for OMSF GPU cloud benchmarking data. The project compares AWS GPU instance performance and cost efficiency for molecular dynamics (MD) and relative binding free energy (RBFE) workloads.

The dashboard is implemented as a [marimo](https://marimo.io/) notebook and can be exported to a static HTML/WASM site found here: [benchmarking.eco-infra.rodeo](https://benchmarking.eco-infra.rodeo).

## Project layout

- `src/benchmarking_analysis/analysis.py` - main marimo notebook/dashboard
- `data/` - pricing, benchmark, and system metadata files used by the notebook
- `scripts/generate_ondemand_price.py` - fetches AWS on-demand instance pricing
- `scripts/export_parquet.py` - exports benchmark data from S3 to local parquet files
- `output/` - generated marimo HTML/WASM site
- `src/benchmarking_analysis/static_views.py` - build-time queries for the Astro prototype
- `scripts/export_static_views.py` - exports every MD/RBFE selection as JSON
- `web/` - Astro static frontend
- `output-astro/` - generated Astro site (separate from the deployed notebook)

## Requirements

- [pixi](https://pixi.sh/)

## Setup

Install dependencies with pixi:

```bash
pixi install
```

## Run the notebook locally

```bash
pixi run marimo edit src/benchmarking_analysis/analysis.py
```

If the notebook needs local data access from its `public` directory, create the development data link:

```bash
pixi run -e dev link
```

## Build the static dashboard

```bash
pixi run build
```

This cleans and regenerates `output/`, then copies the contents of `data/` into `output/public/` for the exported dashboard.

## Build the Astro prototype

Requires Node.js 24 and npm in addition to pixi. Install the frontend dependencies once:

```bash
npm --prefix web ci
```

Then generate all benchmark views and build the static site:

```bash
pixi run build-astro
npm --prefix web run preview
```

The preview command prints the local URL. The generated `output-astro/` directory follows the notebook layout: the original introduction and GPU key, the same four section headings and controls, vertically stacked metrics, a horizontal row of benchmark systems, and About. There is no dashboard header, navigation-card grid, or page navigation. Comparison plots retain the notebook's 120 × 260 px dimensions; scaling plots use 360 × 260 px. Chart rows scroll horizontally on smaller screens. Dropdowns update charts in place without changing pages. D3 renders responsive SVG charts in the browser with theme-aware backgrounds, axes, labels, and bars. Hover a bar for an inline value label and highlighting; tooltips give its full context and winner status. Subtle surface gradients and accents follow the active theme, and transitions respect reduced-motion preferences; SVG descriptions also expose these values to assistive technology. JavaScript is required for charts; there are no benchmark tables.

Python precomputes every selection at build time: currently 22 views and 55 chart datasets. Browser JavaScript only selects and renders these results; there is no browser Python, Worker, API, or request-time analysis. Pricing assumptions and provenance are tucked into a collapsed details section under About.

The build uses **`2026-04-pricing.json`**, matching the notebook rather than automatically selecting a newer file. To explicitly try another snapshot:

```bash
pixi run python scripts/export_static_views.py --pricing-file 2026-05-pricing.json
npm --prefix web run build
```

Use `npm --prefix web run build` after that custom export: `pixi run build-astro` regenerates the default April snapshot. Changes to benchmark data or pricing require another build. Generated files in `web/src/generated/` are replaced by the exporter and are not checked in.

Verify the prototype:

```bash
pixi run test-static
pixi run build-astro
npm --prefix web test
```

Python tests compare every generated view with the original notebook SQL and check median aggregation, complex-phase filtering, tie-breaking, missing/zero prices, and dataset completeness. The frontend smoke test checks the built single-page dashboard, controls, chart containers, and bundled chart code. The PR build workflow runs the Python, static-output, and browser checks, then stages the Astro site through the existing preview deployment workflow. The artifact keeps the existing `site-build` / `site.tar.gz` / `output/` contract so staging also works with the deploy workflow already on `main`. Production push builds and deployment remain on marimo.

Optional browser checks exercise every dropdown selection, D3 rendering, notebook chart layout, mobile horizontal scrolling, and light/dark theme changes:

```bash
pixi run uv run --no-project --with playwright==1.64.1 playwright install chromium
pixi run uv run --no-project --with playwright==1.64.1 python tests/browser_dashboard.py
# Or use an installed Chrome: append --channel chrome to the second command.
```

The existing notebook and its WASM build remain unchanged. This prototype keeps its Python queries separate to avoid breaking that export; parity tests guard against calculation drift until a full migration.

## Regenerate data

The aggregate environment includes the extra dependencies needed to fetch AWS pricing and export benchmark parquet files:

```bash
pixi run -e dev generate
```

This command writes updated files into `data/`. The export script reads benchmark artifacts from S3, so AWS credentials with access to the benchmark bucket may be required.


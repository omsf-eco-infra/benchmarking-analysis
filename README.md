# Benchmarking Analysis

Interactive analysis dashboard for OMSF GPU cloud benchmarking data. The project compares AWS GPU instance performance and cost efficiency for molecular dynamics (MD) and relative binding free energy (RBFE) workloads.

The dashboard is implemented as a [marimo](https://marimo.io/) notebook and can be exported to a static HTML/WASM site found here: [benchmarking.eco-infra.rodeo](https://benchmarking.eco-infra.rodeo).

## Project layout

- `src/benchmarking_analysis/analysis.py` - main marimo notebook/dashboard
- `data/` - pricing, benchmark, and system metadata files used by the notebook
- `scripts/generate_ondemand_price.py` - fetches AWS on-demand instance pricing
- `scripts/export_parquet.py` - exports benchmark data from S3 to local parquet files
- `output/` - generated static site output

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

## Regenerate data

The aggregate environment includes the extra dependencies needed to fetch AWS pricing and export benchmark parquet files:

```bash
pixi run -e dev generate
```

This command writes updated files into `data/`. The export script reads benchmark artifacts from S3, so AWS credentials with access to the benchmark bucket may be required.


import { readFileSync } from "node:fs";
import { resolve } from "node:path";

export type Workload = "md" | "rbfe";
export type ViewKind = "mps" | "systems";
export type ChartMetric = "ns_per_day" | "ns_per_dollar" | "dollars_per_edge";

export interface ViewRow {
  system: string;
  mps_process_count: number;
  instance_type: string;
  total_system_size: number | null;
  ns_per_day: number | null;
  ns_per_dollar: number | null;
  dollars_per_edge: number | null;
  winner_ns_per_day: boolean;
  winner_ns_per_dollar: boolean;
  winner_dollars_per_edge: boolean;
}

export interface ViewChart {
  metric: ChartMetric;
  label: string;
}

export interface View {
  path: string;
  workload: Workload;
  kind: ViewKind;
  selection: number | string;
  title: string;
  charts: ViewChart[];
  rows: ViewRow[];
}

export interface Manifest {
  pricing_file: string;
  rbfe_ns_per_edge: number;
  sources: { file: string; sha256: string }[];
  views: View[];
}

/**
 * Read the manifest written by the Python exporter.
 *
 * The server renders the controls; the client imports the same JSON to draw
 * each precomputed selection without an API.
 */
export function loadManifest(): Manifest {
  const path = resolve(process.cwd(), "src/generated/manifest.json");
  let raw: string;
  try {
    raw = readFileSync(path, "utf8");
  } catch (cause) {
    throw new Error("Run `pixi run export-static` from the repository root first.", { cause });
  }
  return JSON.parse(raw) as Manifest;
}

/** Views for one workload and kind, ordered for a dropdown. */
export function viewsFor(manifest: Manifest, workload: Workload, kind: ViewKind): View[] {
  return manifest.views
    .filter((view) => view.workload === workload && view.kind === kind)
    .sort((a, b) =>
      typeof a.selection === "number" && typeof b.selection === "number"
        ? a.selection - b.selection
        : String(a.selection).localeCompare(String(b.selection)),
    );
}

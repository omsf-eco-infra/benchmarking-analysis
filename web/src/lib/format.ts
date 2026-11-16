import type { ChartMetric, View } from "./manifest";

/** Text used wherever a metric is unavailable. */
export const MISSING = "\u2014";

function isNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

/**
 * Format a finite number with fixed decimals, or return an em dash.
 *
 * @param value - Numeric metric, possibly null/missing.
 * @param digits - Fraction digits to display.
 */
export function formatNumber(value: number | null | undefined, digits = 2): string {
  if (!isNumber(value)) return MISSING;
  return value.toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

/** Format a count (integer) with thousands separators, or an em dash. */
export function formatInteger(value: number | null | undefined): string {
  if (!isNumber(value)) return MISSING;
  return value.toLocaleString("en-US", { maximumFractionDigits: 0 });
}

/** Format a selection value (number or string) for display. */
export function formatSelection(value: number | string): string {
  return typeof value === "number" ? formatInteger(value) : value;
}

/** Preserve the chart titles used in the notebook. */
export function chartTitle(view: View, metric: ChartMetric): string {
  const workload = view.workload.toUpperCase();
  if (view.kind === "mps") {
    return metric === "dollars_per_edge"
      ? `${workload} Cost ($/edge)`
      : `${workload} Performance (${metric === "ns_per_day" ? "ns/day" : "ns/dollar"})`;
  }
  const name = metric === "ns_per_day" ? "throughput"
    : metric === "ns_per_dollar" ? "cost efficiency" : "cost per edge";
  return `${workload} ${name} by instance type: ${view.selection}`;
}

/** Compact number formatting for chart axis ticks. */
export function formatTick(value: number): string {
  const magnitude = Math.abs(value);
  const digits = magnitude >= 1000 ? 0 : magnitude >= 10 ? 1 : 2;
  return value.toLocaleString("en-US", { maximumFractionDigits: digits });
}

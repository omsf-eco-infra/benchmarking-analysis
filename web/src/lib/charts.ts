import { axisBottom, axisLeft, max, scaleBand, scaleLinear, select } from "d3";
import type { ScaleBand, ScaleLinear, Selection } from "d3";
import { formatNumber, formatTick } from "./format";
import type { ChartMetric, View, ViewRow } from "./manifest";
const WINNER_BADGE: Record<ChartMetric, string> = {
  ns_per_day: "best ns/day",
  ns_per_dollar: "best ns/$",
  dollars_per_edge: "lowest $/edge",
};

/** A D3 selection wrapper loose enough to pass SVG and HTML elements around. */
type ChartSelection = Selection<any, any, any, any>;
type XScale = ScaleBand<string>;
type YScale = ScaleLinear<number, number>;

/** Marker on rendered SVGs so smoke tests can confirm the D3 bundle ran. */
export const CHARTS_MARKER = "omsf-d3-charts";

/** Colours read from CSS so charts follow the active light/dark theme. */
export interface ChartTheme {
  text: string;
  muted: string;
  grid: string;
}

/** Read the theme colours that charts should draw with. */
export function readTheme(): ChartTheme {
  const styles = getComputedStyle(document.documentElement);
  const value = (name: string, fallback: string) =>
    styles.getPropertyValue(name).trim() || fallback;
  return {
    text: value("--fg-muted", "#4d5763"),
    muted: value("--chart-muted", "#cbd5e1"),
    grid: value("--border", "#d5dae0"),
  };
}

const INSTANCE_COLORS: Record<string, string> = {
  "g4dn.xlarge": "#4c78a8",
  "g5.xlarge": "#f58518",
  "g6e.xlarge": "#e45756",
};
const MPS_COLORS: Record<string, string> = {
  "1": "#bdd7e7",
  "2": "#6baed6",
  "4": "#2171b5",
};
const FALLBACK_COLORS = ["#b279a2", "#e45756", "#72b7b2", "#9d755d", "#bab0ac"];

/** Stable colour per instance type, shared by every comparison chart. */
export function instanceColor(instance: string): string {
  const known = INSTANCE_COLORS[instance];
  if (known) return known;
  let hash = 0;
  for (const character of instance) {
    hash = (hash * 31 + character.charCodeAt(0)) >>> 0;
  }
  return FALLBACK_COLORS[hash % FALLBACK_COLORS.length];
}

/** Colour per MPS process count, used by the scaling charts. */
export function mpsColor(count: number): string {
  return MPS_COLORS[String(count)] ?? instanceColor(`mps-${count}`);
}

function won(metric: ChartMetric, row: ViewRow): boolean {
  if (metric === "ns_per_day") return row.winner_ns_per_day;
  if (metric === "ns_per_dollar") return row.winner_ns_per_dollar;
  return row.winner_dollars_per_edge;
}

function metricValue(metric: ChartMetric, row: ViewRow): number {
  return Math.max(row[metric] ?? 0, 0);
}

function chartLabel(view: View, metric: ChartMetric): string {
  return view.charts.find((chart) => chart.metric === metric)?.label ?? metric;
}

function barLabel(view: View, metric: ChartMetric, row: ViewRow): string {
  const value = `${row.system}, ${row.instance_type}, MPS ${row.mps_process_count}: ${formatNumber(row[metric])} — ${chartLabel(view, metric)}`;
  return won(metric, row) ? `${value} (${WINNER_BADGE[metric]})` : value;
}

function addHoverValue(bars: ChartSelection, metric: ChartMetric): void {
  bars.on("pointerenter", (event: PointerEvent, row: ViewRow) => {
    const rect = event.currentTarget as SVGRectElement;
    const group = select(rect.parentNode as SVGGElement);
    group.selectAll(".bar-value").remove();
    group.append("text")
      .attr("class", "bar-value")
      .attr("aria-hidden", "true")
      .attr("x", Number(rect.getAttribute("x")) + Number(rect.getAttribute("width")) / 2)
      .attr("y", Number(rect.getAttribute("y")) - 6)
      .attr("text-anchor", "middle")
      .text(formatNumber(row[metric]));
  }).on("pointerleave", (event: PointerEvent) => {
    const rect = event.currentTarget as SVGRectElement;
    select(rect.parentNode as SVGGElement).selectAll(".bar-value").remove();
  });
}

function orderedUnique(values: string[]): string[] {
  return [...new Set(values)];
}

function makeSvg(
  parent: ChartSelection,
  width: number,
  height: number,
  title: string,
) {
  const svg = parent
    .append("svg")
    .attr("role", "img")
    .attr("data-renderer", CHARTS_MARKER)
    .attr("width", width)
    .attr("height", height)
    .attr("viewBox", `0 0 ${width} ${height}`)
    .style("background-color", "var(--surface)");
  svg.append("title").text(title);
  return svg;
}

function appendLegend(
  container: ChartSelection,
  entries: { color: string; label: string }[],
): void {
  const legend = container.append("div").attr("class", "chart-legend");
  legend.append("p").text("MPS processes");
  const list = legend.append("ul");
  for (const entry of entries) {
    const item = list.append("li");
    item.append("span").attr("class", "chart-legend__swatch").style("background", entry.color);
    item.append("span").text(entry.label);
  }
}

interface Margins {
  top: number;
  right: number;
  bottom: number;
  left: number;
}

function drawAxes(
  group: ChartSelection,
  x: XScale,
  y: YScale,
  innerWidth: number,
  innerHeight: number,
  theme: ChartTheme,
  labelAngle = -35,
  showY = true,
): void {
  group
    .append("g")
    .attr("class", "chart-axis chart-axis--x")
    .attr("transform", `translate(0,${innerHeight})`)
    .call(axisBottom(x).tickSize(0).tickPadding(6))
    .call((selection) => selection.select(".domain").attr("stroke", theme.grid))
    .call((selection) =>
      selection
        .selectAll<SVGTextElement, string>("text")
        .attr("transform", `rotate(${labelAngle})`)
        .style("text-anchor", "end")
        .attr("fill", theme.text),
    );

  if (showY) {
    group
      .append("g")
      .attr("class", "chart-axis chart-axis--y")
      .call(
        axisLeft(y)
          .ticks(3)
          .tickFormat((tick) => formatTick(Number(tick)))
          .tickSize(0)
          .tickPadding(6),
      )
      .call((selection) => selection.select(".domain").remove())
      .call((selection) =>
        selection.selectAll<SVGTextElement, number>("text").attr("fill", theme.text),
      );
  }

  group
    .append("line")
    .attr("x1", 0)
    .attr("x2", innerWidth)
    .attr("y1", innerHeight)
    .attr("y2", innerHeight)
    .attr("stroke", theme.grid);
}

function renderComparison(
  container: ChartSelection,
  view: View,
  metric: ChartMetric,
  theme: ChartTheme,
): void {
  // Notebook dimensions: 120px plot width per system and 260px plot height.
  const systems = orderedUnique(view.rows.map((row) => row.system));
  const instances = orderedUnique(view.rows.map((row) => row.instance_type));
  const height = 326;
  const innerHeight = 260;
  const innerWidth = 120;
  const label = chartLabel(view, metric);
  // Shared scales preserve comparisons between systems, as in the notebook.
  const peak = max(view.rows, (row) => metricValue(metric, row)) ?? 0;

  const grid = container.append("div").attr("class", "chart-facets");

  for (const system of systems) {
    const first = system === systems[0];
    const margins: Margins = { top: 8, right: 8, bottom: 58, left: first ? 32 : 8 };
    const facetWidth = innerWidth + margins.left + margins.right;
    const rows = view.rows.filter((row) => row.system === system);
    const facet = grid.append("div").attr("class", "chart-facet");
    const svg = makeSvg(
      facet,
      facetWidth,
      height,
      `${label} for ${system} in ${view.title}`,
    );

    const x = scaleBand().domain(instances).range([0, innerWidth]).padding(0.25);
    const y = scaleLinear()
      .domain([0, peak > 0 ? peak * 1.08 : 1])
      .nice()
      .range([innerHeight, 0]);
    const group = svg
      .append("g")
      .attr("transform", `translate(${margins.left},${margins.top})`);

    drawAxes(group, x, y, innerWidth, innerHeight, theme, -45, first);

    const bars = group
      .append("g")
      .selectAll("rect")
      .data(rows.filter((row) => row[metric] !== null))
      .join("rect")
      .attr("class", "chart-bar")
      .attr("rx", 2)
      .attr("x", (row) => x(row.instance_type) ?? 0)
      .attr("width", x.bandwidth())
      .attr("y", (row) => y(metricValue(metric, row)))
      .attr("height", (row) => innerHeight - y(metricValue(metric, row)))
      .attr("fill", (row) => (won(metric, row) ? instanceColor(row.instance_type) : theme.muted));
    bars.append("title").text((row) => barLabel(view, metric, row));
    addHoverValue(bars, metric);
    svg.append("desc").text(rows.filter((row) => row[metric] !== null)
      .map((row) => barLabel(view, metric, row)).join("; "));
    facet.append("p").attr("class", "chart-facet__label").text(system);
  }
}

function renderScaling(
  container: ChartSelection,
  view: View,
  metric: ChartMetric,
  theme: ChartTheme,
): void {
  // Notebook dimensions: three 120px instance groups, 260px plot height.
  const width = 420;
  const height = 326;
  const margins: Margins = { top: 8, right: 14, bottom: 58, left: 46 };
  const innerWidth = width - margins.left - margins.right;
  const innerHeight = height - margins.top - margins.bottom;
  const instances = orderedUnique(view.rows.map((row) => row.instance_type));
  const counts = orderedUnique(view.rows.map((row) => String(row.mps_process_count)))
    .map(Number)
    .sort((a, b) => a - b);
  const peak = max(view.rows, (row) => metricValue(metric, row)) ?? 0;
  const label = chartLabel(view, metric);

  const plot = container.append("div").attr("class", "scaling-plot");
  const svg = makeSvg(plot, width, height, `${label} across MPS process counts for ${view.title}`);
  if (metric === view.charts[0].metric) {
    appendLegend(plot, counts.map((count) => ({ color: mpsColor(count), label: String(count) })));
  }
  const group = svg.append("g").attr("transform", `translate(${margins.left},${margins.top})`);

  const x = scaleBand().domain(instances).range([0, innerWidth]).paddingInner(0.22);
  const offset = scaleBand()
    .domain(counts.map(String))
    .range([0, x.bandwidth()])
    .padding(0.12);
  const y = scaleLinear()
    .domain([0, peak > 0 ? peak * 1.08 : 1])
    .nice()
    .range([innerHeight, 0]);

  drawAxes(group, x, y, innerWidth, innerHeight, theme);

  const bars = group
    .append("g")
    .selectAll("rect")
    .data(view.rows.filter((row) => row[metric] !== null))
    .join("rect")
    .attr("class", "chart-bar")
    .attr("rx", 2)
    .attr("x", (row) => (x(row.instance_type) ?? 0) + offset(String(row.mps_process_count))!)
    .attr("width", offset.bandwidth())
    .attr("y", (row) => y(metricValue(metric, row)))
    .attr("height", (row) => innerHeight - y(metricValue(metric, row)))
    .attr("fill", (row) => mpsColor(row.mps_process_count));
  bars.append("title").text((row) => barLabel(view, metric, row));
  addHoverValue(bars, metric);
  svg.append("desc").text(view.rows.filter((row) => row[metric] !== null)
    .map((row) => barLabel(view, metric, row)).join("; "));
}

/**
 * Draw one metric for a view into a container, replacing previous content.
 *
 * Comparison views are faceted by benchmark system; scaling views group bars by
 * instance type and colour them by MPS process count.
 */
export function renderChart(
  container: HTMLElement,
  view: View,
  metric: ChartMetric,
  theme: ChartTheme,
): void {
  const root = select(container);
  root.selectAll("*").remove();
  container.dataset.rendered = "true";
  if (view.kind === "mps") {
    renderComparison(root, view, metric, theme);
  } else {
    renderScaling(root, view, metric, theme);
  }
}

import manifestJson from "../generated/manifest.json";
import { readTheme, renderChart } from "../lib/charts";
import { chartTitle } from "../lib/format";
import type { Manifest, View } from "../lib/manifest";

const manifest = manifestJson as Manifest;
const viewsByPath = new Map(manifest.views.map((view) => [view.path, view]));

interface Section {
  root: HTMLElement;
  select: HTMLSelectElement;
}

/** Redraw the fixed metric panels for the selected precomputed view. */
function renderCharts(section: Section, view: View): void {
  const theme = readTheme();
  for (const chart of view.charts) {
    const canvas = section.root.querySelector<HTMLElement>(
      `[data-metric="${chart.metric}"] [data-chart-canvas]`,
    );
    if (canvas) {
      const title = chartTitle(view, chart.metric);
      canvas.setAttribute("aria-label", `${title}; scroll horizontally if needed`);
      const caption = canvas.parentElement?.querySelector("figcaption");
      if (caption) caption.textContent = title;
      renderChart(canvas, view, chart.metric, theme);
    }
  }
}

/** Update one section to show a view: title and charts. */
function showView(section: Section, view: View): void {
  const title = section.root.querySelector<HTMLElement>("[data-selection-title]");
  if (title) title.textContent = view.title;

  renderCharts(section, view);
}

const sections: Section[] = [];
for (const root of document.querySelectorAll<HTMLElement>("section[data-workload][data-kind]")) {
  const select = root.querySelector<HTMLSelectElement>("[data-view-select]");
  if (select) sections.push({ root, select });
}

for (const section of sections) {
  const initial = viewsByPath.get(section.select.value);
  if (initial) showView(section, initial);
  section.select.disabled = false;
  section.select.addEventListener("change", () => {
    const view = viewsByPath.get(section.select.value);
    if (view) showView(section, view);
  });
}

function renderAll(): void {
  for (const section of sections) {
    const view = viewsByPath.get(section.select.value);
    if (view) renderCharts(section, view);
  }
}

let scheduled = false;
function scheduleRender(): void {
  if (scheduled) return;
  scheduled = true;
  requestAnimationFrame(() => {
    scheduled = false;
    renderAll();
  });
}

// Re-render when the layout width changes (responsiveness) or the theme flips.
const main = document.querySelector("main");
if (main) {
  let lastWidth = 0;
  new ResizeObserver((entries) => {
    const width = entries[0].contentRect.width;
    if (Math.abs(width - lastWidth) < 1) return;
    lastWidth = width;
    scheduleRender();
  }).observe(main);
}
const darkMode = window.matchMedia("(prefers-color-scheme: dark)");
darkMode.addEventListener("change", scheduleRender);

scheduleRender();

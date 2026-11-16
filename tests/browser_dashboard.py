"""Exercise the built single-page dashboard with Playwright.

Run after ``pixi run build-astro``. Playwright and its browser are optional
verification tools, not application dependencies. Pass ``--channel chrome``
to use an installed Chrome instead of Playwright's bundled Chromium.
"""

import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from threading import Thread

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]


class QuietHandler(SimpleHTTPRequestHandler):
    """Serve the build without logging every chart asset request."""

    def log_message(self, *args):
        pass


def check_charts(section, view):
    """Verify every D3 bar is bound to the selected precomputed values."""
    for chart in view["charts"]:
        figure = section.locator(f'figure[data-metric="{chart["metric"]}"]')
        actual = figure.locator("svg rect").evaluate_all(
            "els => els.map(el => el.__data__)"
        )
        expected = [row for row in view["rows"] if row[chart["metric"]] is not None]

        def key(row):
            return (row["system"], row["mps_process_count"], row["instance_type"])

        assert sorted(actual, key=key) == sorted(expected, key=key), view["path"]
        tooltips = figure.locator("svg rect > title").all_text_contents()
        assert len(tooltips) == len(expected)
        assert all(
            view["title"] in title
            for title in figure.locator("svg > title").all_text_contents()
        )
        assert figure.locator("svg > desc").count() == figure.locator("svg").count()
        assert all(
            value == "translate(0,260)"
            for value in figure.locator("svg .chart-axis--x").evaluate_all(
                "els => els.map(el => el.getAttribute('transform'))"
            )
        ), "Plot height must match the notebook"
        if view["kind"] == "mps":
            ticks = figure.locator("svg .chart-axis--y").all_text_contents()
            assert len(set(ticks)) == 1, "Comparison facets must share a y scale"
            assert figure.locator(".chart-legend").count() == 0
            panels = figure.locator(".chart-facet").evaluate_all(
                "els => els.map(el => ({top: el.getBoundingClientRect().top, "
                "svgBottom: el.querySelector('svg').getBoundingClientRect().bottom, "
                "labelTop: el.querySelector('.chart-facet__label').getBoundingClientRect().top}))"
            )
            assert len({panel["top"] for panel in panels}) == 1, (
                "Notebook facets must remain one row"
            )
            assert all(panel["labelTop"] >= panel["svgBottom"] for panel in panels)


def check_dashboard(page, manifest, base):
    """Verify selections, charts, resizing, and theme changes without navigation."""
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(base, wait_until="networkidle")
    controls = page.locator("select")
    assert controls.count() == 4
    assert page.locator("section.result-section table").count() == 0
    assert page.locator("header, nav, footer").count() == 0
    assert page.locator("h1").inner_text() == "OMSF GPU Cloud Benchmark Analysis"
    page.wait_for_function("document.querySelectorAll('svg rect').length > 20")
    assert page.locator("svg").count() >= 4
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")

    for index, (workload, kind) in enumerate(
        (("md", "mps"), ("md", "systems"), ("rbfe", "mps"), ("rbfe", "systems"))
    ):
        control = controls.nth(index)
        section = control.locator("xpath=ancestor::section[1]")
        choices = [
            view
            for view in manifest["views"]
            if view["workload"] == workload and view["kind"] == kind
        ]
        options = control.locator("option")
        assert options.count() == len(choices)
        for view in choices:
            # Select by label content only if option values use full view IDs.
            option = next(
                item
                for item in options.evaluate_all(
                    "els => els.map(el => ({value: el.value, text: el.textContent}))"
                )
                if item["value"] in (str(view["selection"]), view["path"])
            )
            control.select_option(value=option["value"])
            check_charts(section, view)
            assert section.locator("svg").count() >= len(view["charts"])
            chart_titles = section.locator("svg > title").all_text_contents()
            assert chart_titles and all(
                view["title"] in title for title in chart_titles
            )
            assert page.url == base, "Selecting a view must not navigate"
        control.select_option(index=0)

    bar = page.locator(".chart-bar").first
    geometry = bar.evaluate(
        "(el) => [el.getAttribute('x'), el.getAttribute('y'), el.getAttribute('width'), el.getAttribute('height')]"
    )
    expected_value = bar.evaluate("(el) => el.__data__.ns_per_day")
    bar.hover()
    assert page.locator(".bar-value").text_content() == f"{expected_value:,.2f}"
    assert geometry == bar.evaluate(
        "(el) => [el.getAttribute('x'), el.getAttribute('y'), el.getAttribute('width'), el.getAttribute('height')]"
    )
    page.mouse.move(1, 1)
    assert page.locator(".bar-value").count() == 0

    assert page.locator(
        "svg rect"
    ).evaluate_all("""rects => rects.every(rect =>
        ['x', 'y', 'width', 'height'].every(name =>
            rect.getAttribute(name) === null || Number.isFinite(Number(rect.getAttribute(name)))))""")
    page.set_viewport_size({"width": 1920, "height": 1000})
    main = page.locator("main").bounding_box()
    assert abs(main["x"] - (1920 - main["width"]) / 2) < 1
    assert page.locator("h1").evaluate("el => getComputedStyle(el).textAlign") == "left"
    assert (
        page.locator(".notebook > p").first.evaluate(
            "el => getComputedStyle(el).textAlign"
        )
        == "left"
    )
    assert page.locator(".section-controls").evaluate_all(
        "els => els.every(el => getComputedStyle(el).justifyContent === 'flex-start')"
    )
    assert page.locator(".chart-facets, .scaling-plot").evaluate_all("""els => els.every(el => {
        const chart = el.getBoundingClientRect();
        const canvas = el.parentElement.getBoundingClientRect();
        return Math.abs(chart.left - canvas.left) < 1;
    })"""), "Graphs must be left-aligned within their panels"

    assert page.locator('[data-kind="systems"] .chart').evaluate_all(
        "els => els.every(el => el.getBoundingClientRect().width <= 720)"
    ), "Small scaling charts should not sit in full-width panels"
    assert page.locator('[data-kind="systems"] .chart__canvas').evaluate_all("""els => els.every(el => {
        const canvas = el.getBoundingClientRect();
        const plot = el.querySelector('svg').getBoundingClientRect();
        return Math.abs(plot.left - canvas.left) < 1;
    })"""), "Scaling plot areas must align even when only one chart has a legend"
    intro = page.locator(".notebook > p").first.bounding_box()
    bullets = page.locator(".notebook > ul").bounding_box()
    assert abs(intro["x"] - bullets["x"]) < 1
    assert page.locator(
        "h1, h2, .notebook > p, .instance-metadata, .section-controls, .chart-grid"
    ).evaluate_all("""els => {
        const edge = document.querySelector('h1').getBoundingClientRect().left;
        return els.every(el => Math.abs(el.getBoundingClientRect().left - edge) < 1);
    }"""), "Page content should share one left edge inside the centered column"

    page.set_viewport_size({"width": 390, "height": 900})
    page.wait_for_function("document.documentElement.scrollWidth <= innerWidth")
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    canvas = page.locator(".chart__canvas").first
    assert canvas.evaluate("(el) => el.scrollWidth > el.clientWidth")
    assert canvas.get_attribute("tabindex") == "0"
    assert canvas.locator(".chart-facets").evaluate(
        "el => Math.abs(el.getBoundingClientRect().left - el.parentElement.getBoundingClientRect().left) < 1"
    ), "Overflowing charts must remain reachable from their left edge"
    panels = page.locator(".chart-facets").first.locator(".chart-facet")
    assert (
        len(
            set(
                panels.evaluate_all(
                    "els => els.map(el => el.getBoundingClientRect().top)"
                )
            )
        )
        == 1
    )

    background = """() => {
        const svg = document.querySelector('svg');
        return [getComputedStyle(svg).backgroundColor,
            getComputedStyle(svg.parentElement).backgroundColor,
            ...Array.from(svg.querySelectorAll('rect')).slice(0, 2).map(r => getComputedStyle(r).fill)];
    }"""
    light = page.evaluate(background)
    page.emulate_media(color_scheme="dark")
    page.wait_for_function(
        f"(before) => JSON.stringify(({background})()) !== JSON.stringify(before)",
        arg=light,
    )
    dark = page.evaluate(background)
    assert light != dark, "Chart background/fills must follow the active theme"
    page.emulate_media(reduced_motion="reduce")
    assert (
        page.locator(".chart-bar").first.evaluate(
            "(el) => getComputedStyle(el).transitionDuration"
        )
        == "0s"
    )
    assert not errors, errors


def main():
    """Serve the static output locally and check it in Chromium."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--channel", default=None)
    args = parser.parse_args()
    manifest = json.loads((ROOT / "web/src/generated/manifest.json").read_text())
    handler = partial(QuietHandler, directory=str(ROOT / "output-astro"))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_port}/"
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(channel=args.channel)
            page = browser.new_page(
                viewport={"width": 1440, "height": 1000}, color_scheme="light"
            )
            check_dashboard(page, manifest, base)
            fallback = browser.new_context(java_script_enabled=False)
            fallback_page = fallback.new_page()
            fallback_page.goto(base)
            assert fallback_page.locator("select:disabled").count() == 4
            assert fallback_page.locator(".noscript-note").is_visible()
            fallback.close()
            browser.close()
    finally:
        server.shutdown()
        server.server_close()
    print(
        "Browser checks passed: all 22 selections, centered column with left-aligned content, hover values, mobile scrolling, themes, and reduced motion."
    )


if __name__ == "__main__":
    main()

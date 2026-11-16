"""Precompute benchmark views for the Astro static prototype."""

import argparse
from pathlib import Path

from benchmarking_analysis.static_views import PRICING_FILE, export_views

ROOT = Path(__file__).resolve().parents[1]


def main():
    """Export every view using an explicit pricing snapshot."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--web-dir", type=Path, default=ROOT / "web")
    parser.add_argument("--pricing-file", default=PRICING_FILE)
    args = parser.parse_args()
    if Path(args.pricing_file).name != args.pricing_file:
        parser.error("--pricing-file must be a filename within --data-dir")
    manifest = export_views(args.data_dir, args.web_dir, args.pricing_file)
    print(
        f"Exported {len(manifest['views'])} views and "
        f"{sum(len(view['charts']) for view in manifest['views'])} chart datasets "
        f"using {manifest['pricing_file']}"
    )


if __name__ == "__main__":
    main()

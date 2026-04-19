from __future__ import annotations

from build_vendor_performance_assets import build_outputs


def main() -> None:
    """Create the curated vendor analysis assets used across the project."""
    outputs = build_outputs()
    for name, path in outputs.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()

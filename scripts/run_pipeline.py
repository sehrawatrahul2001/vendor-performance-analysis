from __future__ import annotations

from bootstrap_data import main as bootstrap_main
from build_vendor_performance_assets import build_outputs
from ingestion_db import build_database


def main() -> None:
    bootstrap_main()
    outputs = build_outputs()
    build_database()
    for name, path in outputs.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()

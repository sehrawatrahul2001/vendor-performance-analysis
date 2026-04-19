from __future__ import annotations

from data_access import bootstrap_vendor_data


def main() -> None:
    resolved = bootstrap_vendor_data()
    for name, path in resolved.items():
        print(f"{name}: {path}")


if __name__ == "__main__":
    main()

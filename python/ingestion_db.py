from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DATABASE_PATH = DATA_DIR / "vendor_performance.db"


def iter_csv_files() -> list[Path]:
    """Return visible CSV files from the data directory."""
    return sorted(
        path
        for path in DATA_DIR.glob("*.csv")
        if not path.name.startswith(".")
    )


def ingest_csv_to_sqlite(csv_path: Path, connection: sqlite3.Connection) -> None:
    """Load a CSV file into SQLite using its filename as the table name."""
    table_name = csv_path.stem.lower().replace(" ", "_")
    dataframe = pd.read_csv(csv_path)
    dataframe.to_sql(table_name, connection, if_exists="replace", index=False)


def build_database() -> Path:
    """Refresh the SQLite database from the CSV assets stored in /data."""
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DATABASE_PATH) as connection:
        for csv_path in iter_csv_files():
            ingest_csv_to_sqlite(csv_path, connection)
    return DATABASE_PATH


if __name__ == "__main__":
    database_path = build_database()
    print(f"SQLite database refreshed at {database_path}")

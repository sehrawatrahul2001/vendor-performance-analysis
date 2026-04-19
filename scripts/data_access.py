from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from urllib.request import urlretrieve


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
SAMPLE_DIR = DATA_DIR / "sample"
PROCESSED_DIR = DATA_DIR / "processed"
DATABASE_PATH = PROCESSED_DIR / "vendor_performance.db"
SOURCE_CONFIG_PATH = DATA_DIR / "data_sources.json"

RAW_FILE_NAMES = [
    "purchases.csv",
    "sales.csv",
    "purchase_prices.csv",
    "vendor_invoice.csv",
    "begin_inventory.csv",
    "end_inventory.csv",
]


def ensure_directory_layout() -> None:
    for directory in (RAW_DIR, SAMPLE_DIR, PROCESSED_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def load_source_config() -> dict[str, object]:
    if not SOURCE_CONFIG_PATH.exists():
        return {}
    return json.loads(SOURCE_CONFIG_PATH.read_text())


def get_local_data_candidates() -> list[Path]:
    config = load_source_config()
    config_dirs = [Path(path).expanduser() for path in config.get("local_data_dirs", []) if path]
    candidates = [
        Path(os.getenv("VENDOR_DATA_DIR", "")).expanduser(),
        Path("/Users/rahulsehrawat/Desktop/Downloads/data"),
        RAW_DIR,
        *config_dirs,
    ]
    return [path for path in candidates if str(path)]


def download_from_google_drive(file_id: str, destination: Path) -> bool:
    if not file_id:
        return False

    drive_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    try:
        urlretrieve(drive_url, destination)
        return destination.exists()
    except Exception:
        result = subprocess.run(
            ["python3", "-m", "gdown", "--fuzzy", drive_url, "-O", str(destination)],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode == 0 and destination.exists()


def try_kaggle_download() -> None:
    config = load_source_config()
    kaggle_config = config.get("kaggle", {})
    if not isinstance(kaggle_config, dict):
        return

    dataset = str(kaggle_config.get("dataset", "")).strip()
    if not dataset:
        return

    subprocess.run(
        ["kaggle", "datasets", "download", "-d", dataset, "-p", str(RAW_DIR), "--unzip", "--force"],
        capture_output=True,
        text=True,
        check=False,
    )


def bootstrap_vendor_data() -> dict[str, Path]:
    ensure_directory_layout()
    resolved: dict[str, Path] = {}
    config = load_source_config()
    direct_urls = config.get("direct_urls", {})
    google_drive_file_ids = config.get("google_drive_file_ids", {})

    for file_name in RAW_FILE_NAMES:
        raw_path = RAW_DIR / file_name
        sample_path = SAMPLE_DIR / file_name.replace(".csv", "_sample.csv")

        if raw_path.exists():
            resolved[file_name] = raw_path
            continue

        for candidate_dir in get_local_data_candidates():
            candidate_path = candidate_dir / file_name
            if candidate_path.exists() and candidate_path != raw_path:
                shutil.copy2(candidate_path, raw_path)
                resolved[file_name] = raw_path
                break
        else:
            source_url = os.getenv(f"VENDOR_{file_name.replace('.csv', '').upper()}_URL", "").strip()
            google_drive_id = str(google_drive_file_ids.get(file_name, "")).strip()
            config_url = str(direct_urls.get(file_name, "")).strip()

            if source_url:
                urlretrieve(source_url, raw_path)
                resolved[file_name] = raw_path
            elif google_drive_id and download_from_google_drive(google_drive_id, raw_path):
                resolved[file_name] = raw_path
            elif config_url:
                urlretrieve(config_url, raw_path)
                resolved[file_name] = raw_path
            elif sample_path.exists():
                resolved[file_name] = sample_path
            else:
                try_kaggle_download()
                if raw_path.exists():
                    resolved[file_name] = raw_path
                    continue
                raise FileNotFoundError(
                    f"Required vendor dataset '{file_name}' not found in raw/, sample/, local paths, "
                    "Google Drive config, direct URL config, or Kaggle bootstrap."
                )

    return resolved


def resolve_input_path(file_name: str) -> Path:
    ensure_directory_layout()
    paths = bootstrap_vendor_data()
    return paths[file_name]

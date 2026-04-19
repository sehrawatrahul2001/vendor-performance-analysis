# Raw Data

The full vendor source files are stored locally in this folder and are intentionally excluded from GitHub because of size.

Supported bootstrap paths:

- Local-first: place the full files anywhere convenient and set `VENDOR_DATA_DIR`, or copy `data/data_sources.example.json` to `data/data_sources.json` and list your local data directories there.
- Google Drive: add file IDs to `data/data_sources.json` and run `python3 scripts/bootstrap_data.py`.
- Kaggle: add the dataset slug to `data/data_sources.json` and run `python3 scripts/bootstrap_data.py`.
- Sample fallback: if no raw source is available, the project will use the GitHub-safe files in `data/sample/`.

Expected raw files:

- `purchases.csv`
- `sales.csv`
- `purchase_prices.csv`
- `vendor_invoice.csv`
- `begin_inventory.csv`
- `end_inventory.csv`

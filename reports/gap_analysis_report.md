# Gap Analysis Report

## Audit Summary

The GitHub-safe version of this project was directionally strong, but it still looked like a partial portfolio case rather than a production-grade analytics repository. The largest issues were data-access friction, outdated repo structure remnants, incomplete inventory usage, and thin reviewer-facing documentation.

## Missing vs Full Project Before Upgrade

### Missing Files

- no self-contained `requirements.txt` inside the project
- no example source configuration file for non-local reviewers
- no generated dashboard asset for GitHub presentation
- no action-oriented watchlist or tier summary output

### Missing Datasets In GitHub

- `purchases.csv`
- `sales.csv`
- `purchase_prices.csv`
- `vendor_invoice.csv`
- `begin_inventory.csv`
- `end_inventory.csv`

These files remain local-only by design because of GitHub size limits.

### Missing Preprocessing Steps

- beginning and ending inventory snapshots were bootstrapped but not used in the analytical scorecard
- inventory-at-risk logic relied only on purchase versus sales deltas
- no explicit sell-through metric in the vendor score

### Missing Automation / Operational Layer

- no configurable Google Drive or Kaggle bootstrap pattern
- no recruiter-facing chart export script

### Missing Dashboards / Reports

- dashboard preview existed, but the repository lacked refreshed PNG outputs tied to the current processed tables
- no ranked executive watchlist for weak-margin vendors

### Missing Documentation

- README did not clearly explain the large-dataset handling strategy
- run instructions were not strong enough for a reviewer who clones the repo without the full raw files

## Upgrades Implemented

- added project-local `requirements.txt`
- added `data/data_sources.example.json` and bootstrap support for local, Google Drive, Kaggle, and sample fallback
- integrated beginning and ending inventory snapshots into the vendor scorecard
- added `SellThroughRate`, ending inventory fields, `vendor_watchlist.csv`, and `vendor_tier_summary.csv`
- generated a recruiter-facing chart asset at `assets/vendor_portfolio_overview.png`
- rewrote the README to be recruiter-friendly and execution-ready

## Remaining Local-Only Elements

- full raw datasets remain intentionally excluded from GitHub
- any heavy BI authoring file such as `.pbix` should remain local unless a lightweight export version is prepared

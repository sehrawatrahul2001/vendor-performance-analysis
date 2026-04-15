# Vendor Performance & Cost Optimization Analysis

This project evaluates supplier performance as a commercial portfolio rather than a simple spend report. It was built by **Rahul Sehrawat**, **Assistant Manager (Operations)**, to demonstrate how procurement, sales, freight, and inventory signals can be converted into vendor decisions that improve margin quality and reduce working-capital pressure.

## Business Objective

The analysis is designed to answer four practical leadership questions:

- which vendors create strong commercial value
- where supplier concentration is creating dependency risk
- which relationships carry cost without proportional return
- where purchasing is running ahead of sell-through

## Project Snapshot

| Area | Details |
|---|---|
| Project Name | Vendor Performance & Cost Optimization Analysis |
| Domain | FMCG / beverage procurement and supplier analytics |
| Source Files | Purchases, sales, pricing, inventory, and invoice data |
| Tools Used | Python, Pandas, SQLite, SQL, Power BI |
| Positioning | Margin analysis, supplier review, inventory risk, cost optimization |

## Workflow

1. Raw procurement, pricing, and sales data is consolidated in Python.
2. Curated vendor-level summary tables are generated for repeatable analysis.
3. SQL translates the curated data into scorecards, watchlists, and executive views.
4. Reports and dashboard notes package the findings for leadership review.

## Headline Insights

- The portfolio generated **$452.1M** in revenue and **$128.5M** in adjusted profit.
- The top 10 suppliers control roughly **65%** of spend and revenue, creating a clear concentration risk.
- Inventory-at-risk totals **$15.6M**, with **1.34M unsold units** still tied up in stock.
- **16 vendors** produce negative adjusted profit and require commercial review.
- Classification **2** converts revenue into margin more effectively than Classification **1**.

## Repository Structure

```text
Vendor Performance & Cost Optimization Analysis/
├── data/
├── python/
├── sql/
├── dashboard/
├── reports/
├── README.md
└── project_storyline.md
```

## Key Files

- `python/build_vendor_performance_assets.py`
- `python/exploratory_data_analysis.ipynb`
- `python/vendor_performance_analysis.ipynb`
- `sql/vendor_performance_cost_optimization_queries.sql`
- `dashboard/dashboard_brief.md`
- `dashboard/vendor_performance_cost_optimization.pbix`
- `reports/business_problem_report.md`
- `reports/final_insights_report.md`

## Why This Project Stands Out

The case study is positioned like a supplier-review engagement rather than a classroom exercise. It reflects Rahul Sehrawat's operations experience in vendor coordination, follow-through, and procurement control, then translates that perspective into structured analytics and decision support.

## Dataset Note

The original datasets used in this project were large and simulated real-world business scenarios.

Due to GitHub file size limitations, full datasets are not included in this repository.

However, all analysis, processed outputs, and insights are fully available to demonstrate the complete workflow and business impact.
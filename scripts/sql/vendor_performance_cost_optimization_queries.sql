-- Vendor Performance & Cost Optimization Analysis
-- Curated source tables:
--   vendor_performance_summary
--   vendor_brand_performance
--   category_performance_summary

/* 1. Executive KPI snapshot */
SELECT
    ROUND(SUM(Revenue), 2) AS total_revenue,
    ROUND(SUM(PurchaseCost), 2) AS total_procurement_cost,
    ROUND(SUM(AdjustedProfit), 2) AS total_adjusted_profit,
    ROUND(SUM(AdjustedProfit) * 100.0 / NULLIF(SUM(Revenue), 0), 2) AS adjusted_margin_pct,
    COUNT(DISTINCT VendorNumber) AS vendor_count
FROM vendor_performance_summary;


/* 2. Full vendor scorecard */
SELECT
    VendorNumber,
    VendorName,
    ROUND(Revenue, 2) AS revenue,
    ROUND(PurchaseCost, 2) AS purchase_cost,
    ROUND(AllocatedFreight, 2) AS allocated_freight,
    ROUND(AdjustedProfit, 2) AS adjusted_profit,
    ROUND(AdjustedMarginPct, 2) AS adjusted_margin_pct,
    ROUND(InventoryAtRisk, 2) AS inventory_at_risk,
    ROUND(StockTurnover, 2) AS stock_turnover,
    ROUND(AvgLeadDays, 2) AS avg_lead_days,
    PerformanceRank,
    PerformanceTier
FROM vendor_performance_summary
ORDER BY AdjustedProfit DESC, Revenue DESC;


/* 3. Top-performing vendors */
SELECT
    VendorName,
    ROUND(Revenue, 2) AS revenue,
    ROUND(PurchaseCost, 2) AS purchase_cost,
    ROUND(AdjustedProfit, 2) AS adjusted_profit,
    ROUND(AdjustedMarginPct, 2) AS adjusted_margin_pct,
    ROUND(PerformanceScore, 2) AS performance_score,
    PerformanceRank,
    PerformanceTier
FROM vendor_performance_summary
ORDER BY PerformanceRank, AdjustedProfit DESC
LIMIT 10;


/* 4. Vendors requiring commercial review */
SELECT
    VendorName,
    ROUND(Revenue, 2) AS revenue,
    ROUND(PurchaseCost, 2) AS purchase_cost,
    ROUND(AdjustedProfit, 2) AS adjusted_profit,
    ROUND(AdjustedMarginPct, 2) AS adjusted_margin_pct,
    ROUND(InventoryAtRisk, 2) AS inventory_at_risk,
    ROUND(AvgLeadDays, 2) AS avg_lead_days
FROM vendor_performance_summary
WHERE Revenue > 0
ORDER BY AdjustedProfit ASC, InventoryAtRisk DESC
LIMIT 10;


/* 5. Cost-heavy suppliers with weaker margin conversion */
SELECT
    VendorName,
    ROUND(PurchaseCost, 2) AS purchase_cost,
    ROUND(CostSharePct, 2) AS cost_share_pct,
    ROUND(Revenue, 2) AS revenue,
    ROUND(RevenueSharePct, 2) AS revenue_share_pct,
    ROUND(AdjustedMarginPct, 2) AS adjusted_margin_pct
FROM vendor_performance_summary
ORDER BY PurchaseCost DESC
LIMIT 10;


/* 6. Vendors creating the largest purchase-to-sales imbalance */
SELECT
    VendorName,
    ROUND(PurchaseQuantity, 0) AS purchased_units,
    ROUND(SalesQuantity, 0) AS sold_units,
    ROUND(UnsoldQuantity, 0) AS unsold_units,
    ROUND(InventoryAtRisk, 2) AS inventory_at_risk,
    ROUND(SalesToPurchaseRatio, 2) AS sales_to_purchase_ratio
FROM vendor_performance_summary
ORDER BY InventoryAtRisk DESC, UnsoldQuantity DESC
LIMIT 10;


/* 7. Category contribution view */
SELECT
    Classification,
    VendorCount,
    BrandCount,
    ROUND(Revenue, 2) AS revenue,
    ROUND(PurchaseCost, 2) AS purchase_cost,
    ROUND(AdjustedProfit, 2) AS adjusted_profit,
    ROUND(GrossMarginPct, 2) AS gross_margin_pct
FROM category_performance_summary
ORDER BY AdjustedProfit DESC, Revenue DESC;


/* 8. Window-function vendor ranking by profit, revenue, and margin */
WITH vendor_rankings AS (
    SELECT
        VendorNumber,
        VendorName,
        Revenue,
        PurchaseCost,
        AdjustedProfit,
        AdjustedMarginPct,
        DENSE_RANK() OVER (ORDER BY AdjustedProfit DESC) AS profit_rank,
        DENSE_RANK() OVER (ORDER BY Revenue DESC) AS revenue_rank,
        DENSE_RANK() OVER (ORDER BY AdjustedMarginPct DESC) AS margin_rank
    FROM vendor_performance_summary
)
SELECT
    VendorNumber,
    VendorName,
    ROUND(Revenue, 2) AS revenue,
    ROUND(PurchaseCost, 2) AS purchase_cost,
    ROUND(AdjustedProfit, 2) AS adjusted_profit,
    ROUND(AdjustedMarginPct, 2) AS adjusted_margin_pct,
    profit_rank,
    revenue_rank,
    margin_rank
FROM vendor_rankings
ORDER BY profit_rank, revenue_rank
LIMIT 15;

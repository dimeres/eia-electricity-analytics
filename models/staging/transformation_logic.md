{% docs retail_sales_cleaning %}
### 🧹 Transformation Logic: Retail Sales
**Goal:** Standardize EIA Retail Sales data for cross-state comparison.

**Key Cleaning Decisions:**
1.  **Filtered Aggregates:** Removed rows where `sectorid` was 'ALL' or 'OTH'.
    * *Reason:* 'ALL' rows are pre-aggregated sums that would cause double-counting in downstream joins. 'OTH' rows were found to be largely null/empty.
2.  **Unit Standardization:**
    * Renamed `sales` → `sales_gwh` (Converted from Million kWh 1:1).
    * Renamed `revenue` → `revenue_mil_usd`.
    * *Outcome:* Dropped 4 redundant metadata columns (`..._units`) to reduce table width.
3.  **Null Handling:** Coalesced numeric metrics to 0 (except Price) to ensure accurate summation in dashboards.
{% enddocs %}

{% docs generation_cleaning %}
### 🧹 Transformation Logic: Generation
**Goal:** Align disparate fuel units into a common comparison metric (GWh).

**Key Cleaning Decisions:**
1.  **Handling "Sector 99":**
    * The dataset is filtered to the "Total Electric Power Industry" (Sector 99).
    * **Crucial:** We explicitly filtered out `fuelTypeid` = 'ALL'/'TOT' to prevent double-counting when summing specific fuels (Coal + Gas + etc).
2.  **Dropped Columns:**
    * **Dropped `cost`:** Analysis revealed >95% null rate (mostly Renewables/IPP). Decided to exclude financial metrics from generation analysis.
3.  **Variable Units:**
    * Kept `consumption_unit_label` because input fuels vary (Tons vs Barrels).
    * Renamed `consumption` → `consumption_fuel_thousands` for clarity.
{% enddocs %}

{% docs emissions_cleaning %}
### 🧹 Transformation Logic: CO2 Emissions
**Goal:** Prepare emissions data for efficiency calculations (Revenue / CO2).

**Key Cleaning Decisions:**
1.  **Granularity Control:**
    * Filtered out 'TOT' (Total Fuels) and 'ALL' (Total Sectors).
    * *Reason:* We need granular data to calculate emissions per specific fuel source.
2.  **Renaming:** Changed generic `value` column to `emissions_million_metric_tons` for immediate user clarity without needing a separate unit column.
{% enddocs %}
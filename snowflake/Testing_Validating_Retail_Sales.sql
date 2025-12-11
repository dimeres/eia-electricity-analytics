/* 
    DATA QUALITY CHECK: SECTOR AGGREGATION VALIDATION

      Verify if the 'ALL' sector rows provided by the EIA are mathematically 
      identical to the sum of the individual sectors (Residential, Commercial, etc.).   
*/
SELECT 
    period,
    
    -- Sum of specific sectors (COM + IND + RES + etc.)
    SUM(CASE WHEN sectorid != 'ALL' THEN customers ELSE 0 END) as calc_sum_customers,
    -- The actual 'ALL' row from EIA
    SUM(CASE WHEN sectorid = 'ALL' THEN customers ELSE 0 END) as actual_all_customers,
    -- The difference (Should be 0)
    (calc_sum_customers - actual_all_customers) as customer_diff,


    -- CHECK 2: SALES (Summable)
    
    SUM(CASE WHEN sectorid != 'ALL' THEN sales ELSE 0 END) as calc_sum_sales,
    SUM(CASE WHEN sectorid = 'ALL' THEN sales ELSE 0 END) as actual_all_sales,
    (calc_sum_sales - actual_all_sales) as sales_diff,



    -- CHECK 3: PRICE 
    -- Simple Average of the sector prices (Unweighted)
    SUM(CASE WHEN sectorid != 'ALL' THEN price * your_weight_column END) 
        / NULLIF(SUM(CASE WHEN sectorid != 'ALL' THEN your_weight_column END), 0) as weighted_avg_sector_price,
    -- The 'ALL' price 
    MAX(CASE WHEN sectorid = 'ALL' THEN price ELSE NULL END) as actual_all_price

FROM raw.retail_sales
WHERE stateid = 'AK'
GROUP BY period
ORDER BY period DESC;


-- others have null values 
select * from raw.retail_sales where sectorid = 'OTH' and price is not null;
with source as (
    select * from {{ source('eia_raw', 'RETAIL_SALES') }}
),

cleaned as (
    select
        -- 1. IDs
        md5(concat(
            coalesce(cast(period as varchar), ''), '-', 
            coalesce(stateid, ''), '-', 
            coalesce(sectorid, '')
        )) as retail_sales_id,

        -- 2. Standardize
        period as report_date,
        stateid as state_code,
        trim(stateDescription) as state_name,
        sectorid as sector_code,
        trim(sectorName) as sector_name,
        
        -- 3. Metrics (Handling NULLs)
        -- Since we filtered out 'OTH' (the empty ones), any remaining NULLs
        -- in valid sectors should be treated as 0.
        coalesce(customers, 0) as customer_count,
        coalesce(revenue, 0) as revenue_mil_usd,
        -- Raw Unit = 'million kilowatt hours'
        -- 1 Million kWh = 1 GWh
        coalesce(sales, 0) as sales_gwh,
        price as price_cents_kwh, -- Keep NULL for accurate averages

    from source
    
    -- FILTERING: Remove aggregates ('ALL') and empty ghosts ('OTH')
    where sectorid != 'ALL'
      and sectorid != 'OTH'
)

select * from cleaned

/* DEDUPLICATION SAFETY NET:
  If the automation script accidentally loads the same data twice 
  this Window Function keeps only the first record per ID and filters out the duplicates. */
qualify row_number() over (partition by retail_sales_id order by report_date) = 1


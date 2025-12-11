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
        coalesce(sales, 0) as sales_mwh,
        price as price_cents_kwh, -- Keep NULL for accurate averages

        -- 4. Units (Shortened)
        case 
            when customers_units = 'number of customers' then '#'
            else customers_units
        end as customer_unit_label,

        case 
            when revenue_units = 'million dollars' then 'M USD'
            else revenue_units
        end as revenue_unit_label,

        case 
            when sales_units = 'million kilowatt hours' then 'MWh'
            else sales_units
        end as sales_unit_label

    from source
    
    -- FILTERING: Remove aggregates ('ALL') and empty ghosts ('OTH')
    where sectorid != 'ALL'
      and sectorid != 'OTH'
)

select * from cleaned

-- DEDUPLICATION
qualify row_number() over (partition by retail_sales_id order by report_date) = 1
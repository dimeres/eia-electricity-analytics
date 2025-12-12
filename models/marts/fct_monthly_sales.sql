select
    -- FOREIGN KEYS
    report_date as date_id,
    state_code as state_id,
    sector_code as sector_id,
    sector_name,


    -- METRICS
    revenue_mil_usd,
    sales_gwh,
    customer_count,
    price_cents_kwh 

from {{ ref('stg_retail_sales') }}


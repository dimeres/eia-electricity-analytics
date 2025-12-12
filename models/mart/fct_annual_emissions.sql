select
    -- FOREIGN KEYS
    -- 1. Date: Create a 'Jan 1st' date anchor so it links to dim_date
    date_from_parts(report_year, 1, 1) as date_id,
    
    
    state_code as state_id,
    sector_code,
    sector_name,

    -- Fuel: Link this to 'co2_group_code' in dim_fuel (The Bridge)
    fuel_code as co2_group_code,

    -- METRICS
    co2_milion_tons

from {{ ref('stg_co2_emissions') }}
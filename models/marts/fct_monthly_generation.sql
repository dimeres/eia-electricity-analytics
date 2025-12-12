select
    -- FOREIGN KEYS (Connect to Dimensions)
    report_date as date_id,
    state_code as state_id,
    fuel_code as fuel_id,

    -- METRICS (The Numbers)
    generation_gwh,
    consumption_fuel_thousands,
    consumption_million_mmbtu,
    
    -- CALCULATED METRIC: Heat Rate (Efficiency)
    -- Formula: MMBtu / MWh. 
    div0(consumption_million_mmbtu, generation_gwh) as heat_rate_mmbtu_per_gwh

from {{ ref('stg_generation') }}
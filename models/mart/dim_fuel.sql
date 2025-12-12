with distinct_fuels as (
    -- Get every single fuel code used in the Generation dataset
    select distinct 
        fuel_code, 
        fuel_desc 
    from {{ ref('stg_generation') }}
)

select 
    fuel_code as fuel_id,
    fuel_desc as fuel_description
from distinct_fuels
where fuel_code is not null

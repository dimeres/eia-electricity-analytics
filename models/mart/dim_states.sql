with all_states as (
    select distinct state_code, state_name from {{ ref('stg_retail_sales') }}
    union
    select distinct state_code, state_name from {{ ref('stg_generation') }}
    union
    select distinct state_code, state_name from {{ ref('stg_co2_emissions') }}
)

select 
    state_code as state_id, -- Primary Key
    state_name
from all_states
where state_code is not null
with source as (
    select * from {{ source('eia_raw', 'GENERATION') }}
),

cleaned as (
    select
        -- 1. IDs
        md5(concat(
            coalesce(cast(period as varchar), ''), '-', 
            coalesce(location, ''), '-', 
            coalesce(fuelTypeid, '')
        )) as generation_id,

        -- 2. Standardize
        period as report_date,
        location as state_code,
        trim(stateDescription) as state_name,
        fuelTypeid as fuel_code,
        trim(fuelTypeDescription) as fuel_desc,

        -- 3. Generation (1,000 MWh = 1 GWh)
        coalesce(generation, 0) as generation_gwh,
        
        -- 4. Consumption (The Fix)
        coalesce(consumption_for_eg, 0) as consumption_fuel_thousands,
        
        -- The Label: "What specific unit is that number?"
        case 
            when consumption_for_eg_units = 'thousand Mcf' then 'Mcf'
            when consumption_for_eg_units = 'thousand barrels' then 'Barrels'
            when consumption_for_eg_units = 'thousand short tons' then 'Tons'
            when consumption_for_eg_units = 'thousand physical units' then 'Units'
            else consumption_for_eg_units 
        end as consumption_unit_label,

            -- Unit: Million MMBtu (Constant). 
            coalesce(consumption_for_eg_btu, 0) as consumption_million_mmbtu
    from source
    
    WHERE 
        (sectorid = '99' OR sectorid = 'ALL') 
        AND fuelTypeid not in ('ALL', 'TOT', 'AFL')
)

select * from cleaned


/* DEDUPLICATION SAFETY NET:
  If the automation script accidentally loads the same data twice 
  this Window Function keeps only the first record per ID and filters out the duplicates. */
qualify row_number() over (partition by generation_id order by report_date) = 1
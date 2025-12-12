with distinct_fuels as (
    -- 1. Source: Get the list of clean, granular fuels
    select distinct 
        fuel_code, 
        fuel_desc,
        consumption_unit_label
    from {{ ref('stg_generation') }}
)

select 
    -- PRIMARY KEY
    fuel_code as fuel_id,
    
    -- DESCRIPTIVE ATTRIBUTES
    fuel_desc as fuel_description,
    consumption_unit_label as fuel_unit, -- (Tons, Barrels, Mcf)

    -- THE BRIDGE: Mapping Granular Fuels -> CO2 Emission Groups
    case 
        -- 1. COAL GROUP (Maps to 'COW')
        -- Includes: Bituminous, Lignite, Subbituminous, Anthracite, Waste Coal, Refined Coal, Synthetic
        when fuel_code in ('ANT', 'BIS', 'BIT', 'LIG', 'RC', 'SUB', 'WOC') then 'COW'
        
        -- 2. NATURAL GAS GROUP (Maps to 'NG')
        -- Includes: Natural Gas only
        when fuel_code in ('NG') then 'NG'
        
        -- 3. PETROLEUM GROUP (Maps to 'PET')
        -- Includes: Distillate, Residual, Pet Coke, Waste Oil
        when fuel_code in ('DFO', 'RFO', 'PC', 'WOO', 'KER') then 'PET'
        
        -- 4. EVERYTHING ELSE (Maps to 'CLEAN')
        -- Includes: Nuclear, Wind, Solar, Hydro, Biomass, Landfill Gas, Geothermal
        else 'CLEAN' 
    end as co2_group_code,

    
    case 
        when fuel_code in ('ANT', 'BIS', 'BIT', 'LIG', 'RC', 'SUB', 'WOC') then 'Coal'
        when fuel_code in ('NG') then 'Natural Gas'
        when fuel_code in ('DFO', 'RFO', 'PC', 'WOO', 'KER') then 'Petroleum'
        when fuel_code in ('NUC') then 'Nuclear'
        when fuel_code in ('WNT', 'WNS') then 'Wind'
        when fuel_code in ('SPV', 'STH', 'DPV') then 'Solar'
        when fuel_code in ('HYC', 'HPS') then 'Hydro'
        when fuel_code in ('BIO', 'LFG', 'MLG', 'OB2', 'OBW', 'MSB', 'WAS', 'WWW') then 'Biomass'
        when fuel_code in ('GEO') then 'Geothermal'
        else 'Other' 
    end as fuel_category_desc

from distinct_fuels
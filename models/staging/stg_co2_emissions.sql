with source as (
    select * from {{ source('eia_raw', 'CO2_EMISSIONS') }}
),

cleaned as (
    select
        -- 1. IDs
        md5(concat(
            coalesce(cast(period as varchar), ''), '-', 
            coalesce(stateid, ''), '-', 
            coalesce(sectorid, ''), '-',
            coalesce(fuelid, '')
        )) as emission_id,

        -- 2. Standardize
        year(period) as report_year,
        stateid as state_code,
        trim(statename) as state_name,
        sectorid as sector_code,
        trim(sectorName) as sector_name,

        -- FUEL MAPPING:
        -- We map CO2 codes to match the Generation dataset codes.
        case 
            when fuelid = 'CO' then 'COW'  -- Map 'Coal' to 'All Coal Products'
            when fuelid = 'PE' then 'PET'  -- Map 'Petroleum' to 'Petroleum'
            else fuelid                    -- Keep 'NG' as 'NG'
        end as fuel_code,

        trim(fuelName) as fuel_desc,
        coalesce(value, 0) as co2_milion_tons

    from source

    WHERE 
        fuelid not in ('TO', 'ALL', 'TOT')
        AND sectorid != 'TT'
)

select * from cleaned

-- DEDUPLICATION SAFETY NET
qualify row_number() over (partition by emission_id order by report_date) = 1
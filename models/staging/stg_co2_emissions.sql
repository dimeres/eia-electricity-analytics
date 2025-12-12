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
        period as report_date,
        stateid as state_code,
        trim(statename) as state_name,
        sectorid as sector_code,
        trim(sectorName) as sector_name,
        fuelid as fuel_code,
        trim(fuelName) as fuel_desc,
        coalesce(value, 0) as co2_milion_tons

    from source

    WHERE 
        fuelid not in ('TO', 'ALL', 'TOT')
        AND sectorid != 'ALL'
)

select * from cleaned

-- DEDUPLICATION SAFETY NET
qualify row_number() over (partition by emission_id order by report_date) = 1
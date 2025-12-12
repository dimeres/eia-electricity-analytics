with range_cte as (
    
    select 
        min(report_date) as min_date,
        max(report_date) as max_date
    from (
        select report_date from {{ ref('stg_retail_sales') }}
        union all
        select report_date from {{ ref('stg_generation') }}
    )
),

date_spine as (
    -- 2. Generate the sequence
    -- Snowflake's generator to create a sequence of numbers
    select 
        dateadd(day, row_number() over (order by null) - 1, min_date) as calendar_date
    from range_cte, table(generator(rowcount => 10000))
    qualify calendar_date <= max_date
)

select
    -- PRIMARY KEY 
    calendar_date as date_id,
    
    -- YEAR
    year(calendar_date) as year_int,                 -- 2010
    date_trunc('year', calendar_date) as year_start, -- 2010-01-01

    -- QUARTER
    quarter(calendar_date) as quarter_int,           -- 1
    'Q' || quarter(calendar_date) as quarter_name,   -- "Q1"

    -- MONTH
    month(calendar_date) as month_int,               -- 1
    monthname(calendar_date) as month_name_short,    -- "Jan"
    to_char(calendar_date, 'MMMM') as month_name_full, -- "January"
    date_trunc('month', calendar_date) as month_start, -- 2010-01-01

    -- WEEK / DAY
    dayofweek(calendar_date) as day_of_week_num,     -- 0-6 (Sun-Sat)
    dayname(calendar_date) as day_of_week_name,      -- "Mon", "Tue"
    
    -- LOGIC FLAGS
    case 
        when dayofweek(calendar_date) in (0, 6) then true 
        else false 
    end as is_weekend

from date_spine
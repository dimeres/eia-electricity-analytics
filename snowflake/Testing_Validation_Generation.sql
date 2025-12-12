
select * from raw.generation limit 15;

select * from raw.generation order by cost ASC limit 15;

-- couldn't idetify how many actuall numbers in this column
-- currently less than 10% are actual numbers and most of it is nulls
select
    sum(case when cost = 0 then 1 else 0 end) as zeroes,
    sum(case when cost is null then 1 else 0 end) as nulls_,
    sum(case when cost is not null and cost <> 0 then 1 else 0 end) as numbers
from raw.generation;


select distinct sectorid from raw.generation; -- should be 99 all the time


-------------------------------
-- Exploring categorical variables
select distinct CONSUMPTION_FOR_EG_UNITS from raw.generation;
/*
    - thousand Mcf
    - thousand physical units
    - thousand barrels
    - thousand short tons
*/

select distinct CONSUMPTION_FOR_EG_UNITS  from raw.generation;
/*
    - thousand Mcf
    - thousand physical units
    - thousand barrels
    - thousand short tons
*/

select distinct GENERATION_UNITS from raw.generation;
/*  
    - thousand megawatthours
*/

-- 43 different type descriptions
select distinct FUELTYPEDESCRIPTION from raw.generation;

select CONSUMPTION_FOR_EG, CONSUMPTION_FOR_EG_BTU
from raw.generation
where fueltypeid = 'OTH';

select distinct  fueltypeid, FUELTYPEDESCRIPTION from generation;

-- there are natural gas, Petroleum, Coal, All fuels

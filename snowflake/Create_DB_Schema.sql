-- Create a database and schema to hold all EIA data
CREATE DATABASE IF NOT EXISTS EIA_DB;
USE DATABASE EIA_DB;
CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;


-- raw tables
CREATE OR REPLACE TABLE RAW.RETAIL_SALES (
    period DATE,
    stateid STRING,
    stateDescription STRING,
    sectorid STRING,
    sectorName STRING,
    customers FLOAT,
    price FLOAT,
    revenue FLOAT,
    sales FLOAT,
    customers_units STRING,
    price_units STRING,
    revenue_units STRING,
    sales_units STRING
);

CREATE OR REPLACE TABLE RAW.GENERATION (
    period DATE,
    location STRING,
    stateDescription STRING,
    sectorid STRING,
    sectorDescription STRING,
    fuelTypeid STRING,
    fuelTypeDescription STRING,
    consumption_for_eg FLOAT,
    consumption_for_eg_units STRING,
    consumption_for_eg_btu FLOAT,
    consumption_for_eg_btu_units STRING,
    cost FLOAT,
    cost_units STRING,
    generation FLOAT,
    generation_units STRING
);


CREATE OR REPLACE TABLE RAW.CO2_EMISSIONS (
    period DATE,
    sectorid STRING,
    sectorName STRING,
    fuelid STRING,
    fuelName STRING,
    stateid STRING,
    statename STRING,
    value FLOAT,
    valueUnits STRING
);



-----------------

CREATE OR REPLACE FILE FORMAT csv_ff
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
TRIM_SPACE = TRUE
NULL_IF = ('', 'NULL')
EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE STAGE raw_stage
FILE_FORMAT = csv_ff;



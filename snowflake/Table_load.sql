-- checking if file format exists
SHOW FILE FORMATS LIKE 'csv_ff';
-- checking files in the stage
LIST @RAW_STAGE;

-- loading data from files into db tables
COPY INTO RAW.RETAIL_SALES
FROM @RAW_STAGE/retail_sales_clean.csv
FILE_FORMAT = (FORMAT_NAME = csv_ff)
ON_ERROR = 'CONTINUE';

COPY INTO RAW.GENERATION
FROM @RAW_STAGE/generation_clean.csv
FILE_FORMAT = (FORMAT_NAME = csv_ff)
ON_ERROR = 'CONTINUE';

COPY INTO RAW.CO2_EMISSIONS
FROM @RAW_STAGE/emissions_clean.csv
FILE_FORMAT = (FORMAT_NAME = csv_ff)
ON_ERROR = 'CONTINUE';


-- validation 
SELECT COUNT(*) FROM RAW.RETAIL_SALES;
SELECT COUNT(*) FROM RAW.GENERATION;
SELECT COUNT(*) FROM RAW.CO2_EMISSIONS;


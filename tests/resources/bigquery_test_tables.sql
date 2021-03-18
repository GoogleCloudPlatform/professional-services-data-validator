
-- BigQuery table with several different numeric datatypes with the same value
CREATE OR REPLACE TABLE pso_data_validator.test_data_types AS
SELECT
    CAST(2 AS BIGNUMERIC) bignumeric_type,
    CAST(2 AS INT64) int_type,
    CAST(2 AS DECIMAL) decimal_type,
    CAST(2 AS STRING) text_type


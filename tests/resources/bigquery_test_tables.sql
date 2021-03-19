
-- BigQuery table with several different numeric datatypes with the same value
CREATE OR REPLACE TABLE pso_data_validator.test_data_types AS
SELECT
    CAST('1234567890123456789012345678901234.00' AS BIGNUMERIC) bignumeric_type,
    CAST(2 AS INT64) int_type,
    CAST(2 AS DECIMAL) decimal_type,
    CAST(2 AS STRING) text_type,
    CAST('2021-01-01 00:00:00' AS TIMESTAMP) timestamp_type


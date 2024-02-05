
-- BigQuery table with several different numeric datatypes with the same value
CREATE OR REPLACE TABLE pso_data_validator.test_data_types AS
SELECT
    CAST('1234567890123456789012345678901234.00' AS BIGNUMERIC) bignumeric_type,
    CAST(2 AS INT64) int_type,
    CAST(2 AS DECIMAL) decimal_type,
    CAST(2 AS STRING) text_type,
    CAST('2021-01-01 00:00:00' AS TIMESTAMP) timestamp_type


-- Core data types test table, to be kept in sync with same table in other SQL engines
CREATE OR REPLACE TABLE `pso_data_validator`.`dvt_core_types`
(   id              INT NOT NULL
,   col_int8        TINYINT
,   col_int16       SMALLINT
,   col_int32       INT
,   col_int64       INT64
,   col_dec_20      NUMERIC(20)
,   col_dec_38      BIGNUMERIC(38)
,   col_dec_10_2    NUMERIC(10,2)
,   col_float32     FLOAT64
,   col_float64     FLOAT64
,   col_varchar_30  STRING(30)
,   col_char_2      STRING(2)
,   col_string      STRING
,   col_date        DATE
,   col_datetime    DATETIME
,   col_tstz        TIMESTAMP
) OPTIONS (description='Core data types integration test table');

INSERT INTO `pso_data_validator`.`dvt_core_types` VALUES
(1,1,1,1,1
 ,NUMERIC '12345678901234567890'
 ,BIGNUMERIC '1234567890123456789012345'
 ,NUMERIC '123.11',123456.1,12345678.1
 ,'Hello DVT','A ','Hello DVT'
 ,DATE '1970-01-01',DATETIME '1970-01-01 00:00:01'
 ,TIMESTAMP '1970-01-01 00:00:01-01:00')
,(2,2,2,2,2
 ,NUMERIC '12345678901234567890'
 ,BIGNUMERIC '1234567890123456789012345'
 ,NUMERIC '123.22',123456.2,12345678.2
 ,'Hello DVT','B ','Hello DVT'
 ,DATE '1970-01-02',DATETIME '1970-01-02 00:00:02'
 ,TIMESTAMP '1970-01-02 00:00:02-02:00')
,(3,3,3,3,3
 ,NUMERIC '12345678901234567890'
 ,BIGNUMERIC '1234567890123456789012345'
 ,NUMERIC '123.33',123456.3,12345678.3
 ,'Hello DVT','C ','Hello DVT'
 ,DATE '1970-01-03',DATETIME '1970-01-03 00:00:03'
 ,TIMESTAMP '1970-01-03 00:00:03-03:00');

DROP TABLE `pso_data_validator`.`dvt_null_not_null`;
CREATE TABLE `pso_data_validator`.`dvt_null_not_null`
(   col_nn             DATETIME NOT NULL
,   col_nullable       DATETIME
,   col_src_nn_trg_n   DATETIME
,   col_src_n_trg_nn   DATETIME NOT NULL
) OPTIONS (description='Nullable integration test table, BigQuery is assumed to be a DVT target (not source)');

DROP TABLE `pso_data_validator`.`dvt_large_decimals`;
CREATE TABLE `pso_data_validator`.`dvt_large_decimals`
(   id              BIGNUMERIC(38) NOT NULL
,   col_data        STRING(10)
,   col_dec_18      INT64
,   col_dec_38      BIGNUMERIC(38)
,   col_dec_38_9    NUMERIC
,   col_dec_38_30   BIGNUMERIC(38,30)
) OPTIONS (description='Large decimals integration test table');

INSERT INTO `pso_data_validator`.`dvt_large_decimals` VALUES
(BIGNUMERIC '123456789012345678901234567890'
,'Row 1'
,123456789012345678
,BIGNUMERIC '12345678901234567890123456789012345678'
,NUMERIC '12345678901234567890123456789.123456789'
,BIGNUMERIC '12345678.123456789012345678901234567890')
,(BIGNUMERIC '223456789012345678901234567890'
,'Row 2'
,223456789012345678
,BIGNUMERIC '22345678901234567890123456789012345678'
,NUMERIC '22345678901234567890123456789.123456789'
,BIGNUMERIC '22345678.123456789012345678901234567890')
,(BIGNUMERIC '323456789012345678901234567890'
,'Row 3'
,323456789012345678
,BIGNUMERIC '32345678901234567890123456789012345678'
,NUMERIC '32345678901234567890123456789.123456789'
,BIGNUMERIC '32345678.123456789012345678901234567890');

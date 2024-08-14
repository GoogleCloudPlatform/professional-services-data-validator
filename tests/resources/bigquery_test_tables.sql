
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
 ,NUMERIC '123.3',123456.3,12345678.3
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

DROP TABLE `pso_data_validator`.`dvt_binary`;
CREATE TABLE `pso_data_validator`.`dvt_binary`
(   binary_id       BYTES(16) NOT NULL
,   int_id          INT64 NOT NULL
,   other_data      STRING(100)
) OPTIONS (description='Integration test table used to test both binary pk matching and binary hash/concat comparisons.');
INSERT INTO `pso_data_validator`.`dvt_binary` VALUES
(CAST('DVT-key-1' AS BYTES), 1, 'Row 1'),
(CAST('DVT-key-2' AS BYTES), 2, 'Row 2'),
(CAST('DVT-key-3' AS BYTES), 3, 'Row 3'),
(CAST('DVT-key-4' AS BYTES), 4, 'Row 4'),
(CAST('DVT-key-5' AS BYTES), 5, 'Row 5');

DROP TABLE `pso_data_validator`.`dvt_string_id`;
CREATE TABLE `pso_data_validator`.`dvt_string_id`
(   id          STRING(15) NOT NULL
,   other_data  STRING(100)
) OPTIONS (description='Integration test table used to test string pk matching.');
INSERT INTO `pso_data_validator`.`dvt_string_id` VALUES
('DVT-key-1', 'Row 1'),
('DVT-key-2', 'Row 2'),
('DVT-key-3', 'Row 3'),
('DVT-key-4', 'Row 4'),
('DVT-key-5', 'Row 5');

DROP TABLE `pso_data_validator`.`dvt_char_id`;
-- BigQuery does not have a specific padded CHAR data type.
CREATE TABLE `pso_data_validator`.`dvt_char_id`
(   id          STRING(6) NOT NULL
,   other_data  STRING(100)
) OPTIONS (description='Integration test table used to test CHAR pk matching.');
INSERT INTO `pso_data_validator`.`dvt_char_id` VALUES
('DVT1  ', 'Row 1'),
('DVT2  ', 'Row 2'),
('DVT3  ', 'Row 3'),
('DVT4  ', 'Row 4'),
('DVT5  ', 'Row 5');

DROP TABLE `pso_data_validator`.`dvt_time_table`;
CREATE TABLE `pso_data_validator`.`dvt_time_table`
(   id          INTEGER NOT NULL
,   col_time  Time
) OPTIONS (description='Integration test table used to test Time data type');
INSERT INTO `pso_data_validator`.`dvt_time_table` VALUES
(1, '00:01:44'),
(2, '04:02:00'),
(3, '08:01:07');

DROP TABLE `pso_data_validator`.`dvt_latin`;
CREATE TABLE `pso_data_validator`.`dvt_latin`
(   id          INT64
,   words       STRING
) OPTIONS (description='Integration test table used to test latin characters.');

INSERT INTO `pso_data_validator`.`dvt_latin` (id, words) VALUES
(1, '2175 BOUL. CURÉ-LABELLE'),
(2, 'CAP-SANTÉ'),
(3, 'GASPÉ'),
(4, 'SAINT-RENÉ'),
(5, 'SAINTE-ANE-DE-LA-PÉ');

DROP TABLE `pso_data_validator`.`dvt_pangrams`;
CREATE TABLE `pso_data_validator`.`dvt_pangrams`
(   id          INT64
,   lang        STRING(100)
,   words       STRING(1000)
,   words_en    STRING(1000)
) OPTIONS (description='Integration test table used to test unicode characters.');
-- Text taken from Wikipedia, we cannot guarantee translations :-)
INSERT INTO `pso_data_validator`.`dvt_pangrams` VALUES
(1,'Hebrew', 'שפן אכל קצת גזר בטעם חסה, ודי',
 'A bunny ate some lettuce-flavored carrots, and he had enough'),
(2,'Polish', 'Pchnąć w tę łódź jeża lub ośm skrzyń fig',
 'Push a hedgehog or eight crates of figs in this boat'),
(3,'Russian', 'Съешь ещё этих мягких французских булок, да выпей же чаю',
 'Eat more of these soft French loaves and drink a tea'),
(4,'Swedish', 'Schweiz för lyxfjäder på qvist bakom ugn',
 'Switzerland brings luxury feather on branch behind oven'),
(5,'Turkish', 'Pijamalı hasta yağız şoföre çabucak güvendi',
 'The sick person in pyjamas quickly trusted the swarthy driver');

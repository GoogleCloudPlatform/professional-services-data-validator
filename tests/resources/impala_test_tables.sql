-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE DATABASE IF NOT EXISTS `pso_data_validator`;

DROP TABLE IF EXISTS `pso_data_validator`.`dvt_core_types`;
CREATE TABLE `pso_data_validator`.`dvt_core_types`
(   id              int
,   col_int8        tinyint
,   col_int16       smallint
,   col_int32       int
,   col_int64       bigint
,   col_dec_20      decimal(20)
,   col_dec_38      decimal(38)
,   col_dec_10_2    decimal(10,2)
,   col_float32     float
,   col_float64     double
,   col_varchar_30  varchar(30)
,   col_char_2      char(2)
,   col_string      string
,   col_date        date
,   col_datetime    timestamp
,   col_tstz        timestamp
)
STORED AS PARQUET
TBLPROPERTIES ('comment'='Core data types integration test table');

INSERT INTO `pso_data_validator`.`dvt_core_types` VALUES
(1,1,1,1,1
 ,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1
 ,CAST('Hello DVT' AS varchar(30)),CAST('A ' AS char(2)),'Hello DVT'
 ,'1970-01-01','1970-01-01 00:00:01','1970-01-01 01:00:01')
,(2,2,2,2,2
 ,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2
 ,CAST('Hello DVT' AS varchar(30)),CAST('B ' AS char(2)),'Hello DVT'
 ,'1970-01-02','1970-01-02 00:00:02','1970-01-02 02:00:02')
,(3,3,3,3,3
 ,12345678901234567890,1234567890123456789012345,123.3,123456.3,12345678.3
 ,CAST('Hello DVT' AS varchar(30)),CAST('C ' AS char(2)),'Hello DVT'
 ,'1970-01-03','1970-01-03 00:00:03','1970-01-03 03:00:03');

CREATE VIEW `pso_data_validator`.`dvt_core_types_vw` AS
SELECT * FROM `pso_data_validator`.`dvt_core_types`;

DROP TABLE IF EXISTS `pso_data_validator`.`dvt_pangrams`;
CREATE TABLE `pso_data_validator`.`dvt_pangrams`
(   id          int
,   lang        varchar(100)
,   words       varchar(1000)
,   words_en    varchar(1000)
)
STORED AS PARQUET
TBLPROPERTIES ('comment'='Integration test table used to test unicode characters.');
-- Text taken from Wikipedia, we cannot guarantee translations :-)
INSERT INTO `pso_data_validator`.`dvt_pangrams` VALUES
(1,cast('Hebrew' as varchar(100)),
 cast('שפן אכל קצת גזר בטעם חסה, ודי' as varchar(1000)),
 cast('A bunny ate some lettuce-flavored carrots, and he had enough' as varchar(1000))),
(2,cast('Polish' as varchar(100)),
 cast('Pchnąć w tę łódź jeża lub ośm skrzyń fig' as varchar(1000)),
 cast('Push a hedgehog or eight crates of figs in this boat' as varchar(1000))),
(3,cast('Russian' as varchar(100)),
 cast('Съешь ещё этих мягких французских булок, да выпей же чаю' as varchar(1000)),
 cast('Eat more of these soft French loaves and drink a tea' as varchar(1000))),
(4,cast('Swedish' as varchar(100)),
 cast('Schweiz för lyxfjäder på qvist bakom ugn' as varchar(1000)),
 cast('Switzerland brings luxury feather on branch behind oven' as varchar(1000))),
(5,cast('Turkish' as varchar(100)),
 cast('Pijamalı hasta yağız şoföre çabucak güvendi' as varchar(1000)),
 cast('The sick person in pyjamas quickly trusted the swarthy driver' as varchar(1000)));

DROP TABLE IF EXISTS `pso_data_validator`.`dvt_bool`;
CREATE TABLE `pso_data_validator`.`dvt_bool`
(   id           int
,   col_bool_dec boolean
,   col_bool_int boolean
,   col_bool_ch1 boolean
,   col_bool_chy boolean
) COMMENT 'Integration test table used to test boolean data type, especially in non-boolean columns.';
INSERT INTO `pso_data_validator`.`dvt_bool` VALUES
(1,true,true,true,true),(2,false,false,false,false);

DROP TABLE IF EXISTS `pso_data_validator`.`test_generate_partitions`;
CREATE TABLE `pso_data_validator`.`test_generate_partitions` (
    course_id string
,   quarter_id int
,   student_id int
,   grade float)
STORED AS PARQUET
TBLPROPERTIES ('comment'='Table for testing generate table partitions, consists of 32 rows with a composite primary key');

INSERT INTO `pso_data_validator`.`test_generate_partitions`
(course_id, quarter_id, student_id, grade) VALUES
('ALG001',1,1234,2.1),('ALG001',1,5678,3.5),('ALG001',1,9012,2.3)
,('ALG001',2,1234,3.5),('ALG001',2,5678,2.6),('ALG001',2,9012,3.5)
,('ALG001',3,1234,2.7),('ALG001',3,5678,3.5),('ALG001',3,9012,2.8)
,('GEO001',1,1234,2.1),('GEO001',1,5678,3.5),('GEO001',1,9012,2.3)
,('GEO001',2,1234,3.5),('GEO001',2,5678,2.6),('GEO001',2,9012,3.5)
,('GEO001',3,1234,2.7),('GEO001',3,5678,3.5),('GEO001',3,9012,2.8)
,('TRI001',1,1234,2.1),('TRI001',1,5678,3.5),('TRI001',1,9012,2.3)
,('TRI001',2,1234,3.5),('TRI001',2,5678,2.6),('TRI001',2,9012,3.5)
,('TRI001',3,1234,2.7),('TRI001',3,5678,3.5),('TRI001',3,9012,2.8);

 /* Following table used for validating generating table partitions  version 2*/
DROP TABLE IF EXISTS `pso_data_validator`.`test_generate_partitions_v2` ;
CREATE TABLE `pso_data_validator`.`test_generate_partitions_v2` (
        course_id STRING,
        quarter_id INTEGER,
        recd_timestamp TIMESTAMP,
        registration_date DATE,
        approved Boolean,
        grade DECIMAL(5,2))
STORED AS PARQUET
TBLPROPERTIES ('comment'= 'Table for testing generate table partitions,
  consists of 32 rows with a composite primary key
  Quoted Strings are handled correctly');

INSERT INTO `pso_data_validator`.`test_generate_partitions_v2` (course_id, quarter_id, recd_timestamp, registration_date, approved, grade) VALUES
        ('ALG001', 1234, '2023-08-26 4:00pm', '1969-07-20', True, 3.5),
        ('ALG001', 1234, '2023-08-26 4:00pm', '1969-07-20', False, 2.8),
        ('ALG001', 5678, '2023-08-26 4:00pm', '2023-08-23', True, 2.1),
        ('ALG001', 5678, '2023-08-26 4:00pm', '2023-08-23', False, 3.5),
        ('ALG003', 1234, '2023-08-27 3:00pm', '1969-07-20', True, 3.5),
        ('ALG003', 1234, '2023-08-27 3:00pm', '1969-07-20', False, 2.8),
        ('ALG003', 5678, '2023-08-27 3:00pm', '2023-08-23', True, 2.1),
        ('ALG003', 5678, '2023-08-27 3:00pm', '2023-08-23', False, 3.5),
        ('ALG002', 1234, '2023-08-26 4:00pm', '1969-07-20', True, 3.5),
        ('ALG002', 1234, '2023-08-26 4:00pm', '1969-07-20', False, 2.8),
        ('ALG002  t0.', 5678, '2023-08-26 4:00pm', '2023-08-23', True, 2.1),
        ('ALG002', 5678, '2023-08-26 4:00pm', '2023-08-23', False, 3.5),
        ('ALG004', 1234, '2023-08-27 3:00pm', '1969-07-20', True, 3.5),
        ('ALG004', 1234, '2023-08-27 3:00pm', '1969-07-20', False, 2.8),
        ('ALG004', 5678, '2023-08-27 3:00pm', '2023-08-23', True, 2.1),
        ('ALG004', 5678, '2023-08-27 3:00pm', '2023-08-23', False, 3.5),
        ('St. John''s', 1234, '2023-08-26 4:00pm', '1969-07-20', True, 3.5),
        ('St. John''s', 1234, '2023-08-26 4:00pm', '1969-07-20', False, 2.8),
        ('St. John''s', 5678, '2023-08-26 4:00pm', '2023-08-23', True, 2.1),
        ('St. John''s', 5678, '2023-08-26 4:00pm', '2023-08-23', False, 3.5),
        ('St. Jude''s', 1234, '2023-08-27 3:00pm', '1969-07-20', True, 3.5),
        ('St. Jude''s', 1234, '2023-08-27 3:00pm', '1969-07-20', False, 2.8),
        ('St. Jude''s', 5678, '2023-08-27 3:00pm', '2023-08-23', True, 2.1),
        ('St. Jude''s', 5678, '2023-08-27 3:00pm', '2023-08-23', False, 3.5),
        ('St. Edward''s', 1234, '2023-08-26 4:00pm', '1969-07-20', True, 3.5),
        ('St. Edward''s', 1234, '2023-08-26 4:00pm', '1969-07-20', False, 2.8),
        ('St. Edward''s', 5678, '2023-08-26 4:00pm', '2023-08-23', True, 2.1),
        ('St. Edward''s', 5678, '2023-08-26 4:00pm', '2023-08-23', False, 3.5),
        ('St. Paul''s', 1234, '2023-08-27 3:00pm', '1969-07-20', True, 3.5),
        ('St. Paul''s', 1234, '2023-08-27 3:00pm', '1969-07-20', False, 2.8),
        ('St. Paul''s', 5678, '2023-08-27 3:00pm', '2023-08-23', True, 2.1),
        ('St. Paul''s', 5678, '2023-08-27 3:00pm', '2023-08-23', False, 3.5);

DROP TABLE IF EXISTS pso_data_validator.dvt_tricky_dates;
CREATE TABLE pso_data_validator.dvt_tricky_dates (
  id            bigint
, col_dt_low    date
, col_dt_epoch  date
, col_dt_high   date
, col_dt_4712   date
, col_ts_low    timestamp
, col_ts_epoch  timestamp
, col_ts_high   timestamp
, col_ts_4712   timestamp)
STORED AS PARQUET
TBLPROPERTIES ('comment'='Integration test table used to test potentially difficult timestamps.');
INSERT INTO pso_data_validator.dvt_tricky_dates VALUES
(1,'1000-01-01','1970-01-01','9999-12-31','4712-12-31'
,'1000-01-01 00:00:00','1970-01-01 00:00:00','9999-12-31 23:59:59','4712-12-31 23:23:59');
-- NULL in all columns.
INSERT INTO pso_data_validator.dvt_tricky_dates (id) VALUES (2);
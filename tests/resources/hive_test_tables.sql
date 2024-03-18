-- Copyright 2023 Google LLC
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

CREATE DATABASE IF NOT EXISTS pso_data_validator;
DROP TABLE IF EXISTS pso_data_validator.dvt_core_types;
CREATE TABLE pso_data_validator.dvt_core_types
(   id              int NOT NULL
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
STORED AS ORC
TBLPROPERTIES ('comment'='Core data types integration test table');

INSERT INTO pso_data_validator.dvt_core_types VALUES
(1,1,1,1,1
 ,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1
 ,'Hello DVT','A ','Hello DVT'
 ,'1970-01-01','1970-01-01 00:00:01','1970-01-01 01:00:01')
,(2,2,2,2,2
 ,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2
 ,'Hello DVT','B ','Hello DVT'
 ,'1970-01-02','1970-01-02 00:00:02','1970-01-02 02:00:02')
,(3,3,3,3,3
 ,12345678901234567890,1234567890123456789012345,123.3,123456.3,12345678.3
 ,'Hello DVT','C ','Hello DVT'
 ,'1970-01-03','1970-01-03 00:00:03','1970-01-03 03:00:03');


DROP TABLE `pso_data_validator`.`dvt_null_not_null`;
CREATE TABLE `pso_data_validator`.`dvt_null_not_null`
(   col_nn             timestamp NOT NULL
,   col_nullable       timestamp
,   col_src_nn_trg_n   timestamp NOT NULL
,   col_src_n_trg_nn   timestamp
) COMMENT 'Nullable integration test table, Hive is assumed to be a DVT source (not target).';

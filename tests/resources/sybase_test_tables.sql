-- Copyright 2025 Google LLC
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

-- Core data types test table, to be kept in sync with same table in other SQL engines
DROP TABLE pso_data_validator.dvt_core_types
GO

CREATE TABLE pso_data_validator.dvt_core_types
(   id              int NOT NULL PRIMARY KEY
,   col_int8        tinyint
,   col_int16       smallint
,   col_int32       int
,   col_int64       bigint
,   col_dec_20      decimal(20)
,   col_dec_38      decimal(38)
,   col_dec_10_2    decimal(10,2)
,   col_float32     real
,   col_float64     float
,   col_varchar_30  varchar(30)
,   col_char_2      char(2)
,   col_string      text
,   col_date        date
,   col_datetime    bigdatetime
,   col_tstz        bigdatetime
)
GO

INSERT INTO pso_data_validator.dvt_core_types VALUES
(1,1,1,1,1
,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1
,'Hello DVT','A ','Hello DVT'
,'1970-01-01','1970-01-01 00:00:01','1970-01-01 01:00:01')
GO
INSERT INTO pso_data_validator.dvt_core_types VALUES
(2,2,2,2,2
,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2
,'Hello DVT','B ','Hello DVT'
,'1970-01-02','1970-01-02 00:00:02','1970-01-02 02:00:02')
GO
INSERT INTO pso_data_validator.dvt_core_types VALUES
(3,3,3,3,3
,12345678901234567890,1234567890123456789012345,123.3,123456.3,12345678.3
,'Hello DVT','C ','Hello DVT'
,'1970-01-03','1970-01-03 00:00:03','1970-01-03 03:00:03')
GO

-- Nullable integration test table, Sybase is assumed to be a DVT source (not target)
DROP TABLE pso_data_validator.dvt_null_not_null
GO

CREATE TABLE pso_data_validator.dvt_null_not_null
(   col_nn             date NOT NULL
,   col_nullable       date
,   col_src_nn_trg_n   date NOT NULL
,   col_src_n_trg_nn   date
)
GO

-- Large decimals integration test table.
DROP TABLE pso_data_validator.dvt_large_decimals
GO

CREATE TABLE pso_data_validator.dvt_large_decimals
(   id                decimal(38) NOT NULL PRIMARY KEY
,   col_data          varchar(10)
,   col_dec_18        decimal(18)
,   col_dec_38        decimal(38)
,   col_dec_38_9      decimal(38,9)
,   col_dec_38_30     decimal(38,30)
-- Columns with mismatched data for intentional fail status.
,   col_dec_18_fail   decimal(18)
,   col_dec_18_1_fail decimal(18,1)
)
GO

INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(123456789012345678901234567890,'Row 1'
,987654321012345678
,12345678901234567890123456789012345678
,12345678901234567890123456789.123456789
,12345678.123456789012345678901234567890
,987654321012345678,12345678901234567.1)
GO
INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(223456789012345678901234567890,'Row 2'
,987654321012345678
,12345678901234567890123456789012345678
,12345678901234567890123456789.123456789
,12345678.123456789012345678901234567890
,987654321012345678,12345678901234567.1)
GO
INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(323456789012345678901234567890,'Row 3'
,987654321012345678
,12345678901234567890123456789012345678
,12345678901234567890123456789.123456789
,12345678.123456789012345678901234567890
,987654321012345678,12345678901234567.1)
GO
INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(423456789012345678901234567890,'Row 4'
,987654321012345678
,12345678901234567890123456789012345678
,12345678901234567890123456789.123456789
,12345678.123456789012345678901234567890
,987654321012345678,12345678901234567.1)
GO
INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(523456789012345678901234567890,'Row 5'
,987654321012345678
,12345678901234567890123456789012345678
,12345678901234567890123456789.123456789
,12345678.123456789012345678901234567890
,987654321012345678,12345678901234567.1)
GO

--Integration test table used to test both binary pk matching and binary hash/concat comparisons.
DROP TABLE pso_data_validator.dvt_binary
GO

CREATE TABLE pso_data_validator.dvt_binary
(   binary_id       varbinary(16) NOT NULL PRIMARY KEY
,   int_id          int NOT NULL
,   other_data      varchar(100)
)
GO

CREATE UNIQUE INDEX dvt_binary_int_id_uk ON pso_data_validator.dvt_binary (int_id)
GO
INSERT INTO pso_data_validator.dvt_binary VALUES (CAST('DVT-key-1' AS binary), 1, 'Row 1')
GO
INSERT INTO pso_data_validator.dvt_binary VALUES (CAST('DVT-key-2' AS binary), 2, 'Row 2')
GO
INSERT INTO pso_data_validator.dvt_binary VALUES (CAST('DVT-key-3' AS binary), 3, 'Row 3')
GO
INSERT INTO pso_data_validator.dvt_binary VALUES (CAST('DVT-key-4' AS binary), 4, 'Row 4')
GO
INSERT INTO pso_data_validator.dvt_binary VALUES (CAST('DVT-key-5' AS binary), 5, 'Row 5')
GO

--Integration test table used to test fixed char pk matching. Trailing blanks are not significant.
DROP TABLE IF EXISTS pso_data_validator.dvt_fixed_char_id
GO

CREATE TABLE pso_data_validator.dvt_fixed_char_id
(   id          char(6) NOT NULL PRIMARY KEY
,   other_data  char(100)
)
GO

INSERT INTO pso_data_validator.dvt_fixed_char_id VALUES ('DVT1', 'Row 1	  ')
GO
INSERT INTO pso_data_validator.dvt_fixed_char_id VALUES ('DVT2', 'Row 2  	')
GO
INSERT INTO pso_data_validator.dvt_fixed_char_id VALUES ('DVT3', 'Row 3  ')
GO
INSERT INTO pso_data_validator.dvt_fixed_char_id VALUES ('DVT4', 'Row 4  	  ')
GO
INSERT INTO pso_data_validator.dvt_fixed_char_id VALUES ('DVT5', 'Row 5')
GO

--Integration test table used to test varchar pk matching. Trailing blanks are significant.
DROP TABLE IF EXISTS pso_data_validator.dvt_varchar_id
GO

CREATE TABLE pso_data_validator.dvt_varchar_id
(   id          VARCHAR(15) NOT NULL PRIMARY KEY
,   other_data  VARCHAR(100)
)
GO

INSERT INTO pso_data_validator.dvt_varchar_id VALUES ('DVT-key-1', 'Row 1')
GO
INSERT INTO pso_data_validator.dvt_varchar_id VALUES ('DVT-key-2', 'Row 2')
GO
INSERT INTO pso_data_validator.dvt_varchar_id VALUES ('DVT-key-3', 'Row 3')
GO
INSERT INTO pso_data_validator.dvt_varchar_id VALUES ('DVT-key-4 ', 'Row 4')
GO
INSERT INTO pso_data_validator.dvt_varchar_id VALUES ('DVT-key-5', 'Row 5')
GO

--Integration test table used to test datetime pk matching', 'SCHEMA', 'pso_data_validator', 'table', 'dvt_datetime_id'
DROP TABLE pso_data_validator.dvt_datetime_id
GO

CREATE TABLE pso_data_validator.dvt_datetime_id
(   id          datetime NOT NULL PRIMARY KEY
,   other_data  varchar(100)
)
GO

INSERT INTO pso_data_validator.dvt_datetime_id VALUES ('2020-01-01 12:00:00', 'Row 1')
GO
INSERT INTO pso_data_validator.dvt_datetime_id VALUES ('2020-02-01 12:00:00', 'Row 2')
GO
INSERT INTO pso_data_validator.dvt_datetime_id VALUES ('2020-03-01 12:00:00', 'Row 3')
GO
INSERT INTO pso_data_validator.dvt_datetime_id VALUES ('2020-04-01 12:00:00', 'Row 4')
GO
INSERT INTO pso_data_validator.dvt_datetime_id VALUES ('2020-05-01 12:00:00', 'Row 5')
GO

--Integration test table used to test unicode characters.

-- TODO Need to dig into character sets to create this table/test:
--   Error converting characters into server's character set. Some character(s) could not be converted
--DROP TABLE pso_data_validator.dvt_pangrams
--GO

--CREATE TABLE pso_data_validator.dvt_pangrams
--(   id          int NOT NULL PRIMARY KEY
--,   lang        varchar(100)
--,   words       nvarchar(1000)
--,   words_en    varchar(1000)
--)
--GO

-- Text taken from Wikipedia, we cannot guarantee translations :-)
--INSERT INTO pso_data_validator.dvt_pangrams VALUES (1,'Hebrew', 'שפן אכל קצת גזר בטעם חסה, ודי', 'A bunny ate some lettuce-flavored carrots, and he had enough')
--GO
--INSERT INTO pso_data_validator.dvt_pangrams VALUES (2,'Polish', 'Pchnąć w tę łódź jeża lub ośm skrzyń fig', 'Push a hedgehog or eight crates of figs in this boat')
--GO
--INSERT INTO pso_data_validator.dvt_pangrams VALUES (3,'Russian', 'Съешь ещё этих мягких французских булок, да выпей же чаю', 'Eat more of these soft French loaves and drink a tea')
--GO
--INSERT INTO pso_data_validator.dvt_pangrams VALUES (4,'Swedish', 'Schweiz för lyxfjäder på qvist bakom ugn', 'Switzerland brings luxury feather on branch behind oven')
--GO
--INSERT INTO pso_data_validator.dvt_pangrams VALUES (5,'Turkish', 'Pijamalı hasta yağız şoföre çabucak güvendi', 'The sick person in pyjamas quickly trusted the swarthy driver')
--GO

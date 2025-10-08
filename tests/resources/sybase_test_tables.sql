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
,   col_int8        tinyint NULL
,   col_int16       smallint NULL
,   col_int32       int NULL
,   col_int64       bigint NULL
,   col_dec_20      decimal(20) NULL
,   col_dec_38      decimal(38) NULL
,   col_dec_10_2    decimal(10,2) NULL
,   col_float32     real NULL
,   col_float64     float NULL
,   col_varchar_30  varchar(30) NULL
,   col_char_2      char(2) NULL
,   col_string      text NULL
,   col_date        date NULL
,   col_datetime    bigdatetime NULL
,   col_tstz        bigdatetime NULL
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

CREATE OR REPLACE VIEW pso_data_validator.dvt_core_types_vw AS
SELECT * FROM pso_data_validator.dvt_core_types
GO

-- Nullable integration test table, Sybase is assumed to be a DVT source (not target)
DROP TABLE pso_data_validator.dvt_null_not_null
GO

CREATE TABLE pso_data_validator.dvt_null_not_null
(   col_nn             bigdatetime NOT NULL
,   col_nullable       bigdatetime NULL
,   col_src_nn_trg_n   bigdatetime NOT NULL
,   col_src_n_trg_nn   bigdatetime NULL
)
GO

-- Large decimals integration test table.
DROP TABLE pso_data_validator.dvt_large_decimals
GO

CREATE TABLE pso_data_validator.dvt_large_decimals
(   id                decimal(38) NOT NULL PRIMARY KEY
,   col_data          varchar(10) NULL
,   col_dec_18        decimal(18) NULL
,   col_dec_38        decimal(38) NULL
,   col_dec_38_9      decimal(38,9) NULL
,   col_dec_38_30     decimal(38,30) NULL
-- Columns with mismatched data for intentional fail status.
,   col_dec_18_fail   decimal(18) NULL
,   col_dec_18_1_fail decimal(18,1) NULL
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
,   other_data      varchar(100) NULL
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
DROP TABLE pso_data_validator.dvt_fixed_char_id
GO

CREATE TABLE pso_data_validator.dvt_fixed_char_id
(   id          char(6) NOT NULL PRIMARY KEY
,   other_data  char(100) NULL
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
DROP TABLE pso_data_validator.dvt_varchar_id
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
--,   lang        varchar(100) NULL
--,   words       nvarchar(1000) NULL
--,   words_en    varchar(1000) NULL
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

--Integration test table used to test validating many columns.
--This table has a reduced column count compared to other engines we test due to the message below:
--  Msg 1767 (severity 16, state 1) from MYSYBASE Line 1:
--	"Number of variable length columns exceeds limit of 254 for allpage locked tables. CREATE TABLE for 'dvt_many_cols' failed. "
DROP TABLE pso_data_validator.dvt_many_cols
GO

CREATE TABLE pso_data_validator.dvt_many_cols
( id decimal(5) NOT NULL PRIMARY KEY
, col_001 varchar(2) NULL
, col_002 varchar(2) NULL
, col_003 varchar(2) NULL
, col_004 varchar(2) NULL
, col_005 varchar(2) NULL
, col_006 varchar(2) NULL
, col_007 varchar(2) NULL
, col_008 varchar(2) NULL
, col_009 varchar(2) NULL
, col_010 varchar(2) NULL
, col_011 decimal(1) NULL
, col_012 decimal(1) NULL
, col_013 decimal(1) NULL
, col_014 decimal(1) NULL
, col_015 decimal(1) NULL
, col_016 decimal(1) NULL
, col_017 decimal(1) NULL
, col_018 decimal(1) NULL
, col_019 decimal(1) NULL
, col_020 decimal(1) NULL
, col_021 varchar(2) NULL
, col_022 varchar(2) NULL
, col_023 varchar(2) NULL
, col_024 varchar(2) NULL
, col_025 varchar(2) NULL
, col_026 varchar(2) NULL
, col_027 varchar(2) NULL
, col_028 varchar(2) NULL
, col_029 varchar(2) NULL
, col_030 varchar(2) NULL
, col_031 decimal(1) NULL
, col_032 decimal(1) NULL
, col_033 decimal(1) NULL
, col_034 decimal(1) NULL
, col_035 decimal(1) NULL
, col_036 decimal(1) NULL
, col_037 decimal(1) NULL
, col_038 decimal(1) NULL
, col_039 decimal(1) NULL
, col_040 decimal(1) NULL
, col_041 varchar(2) NULL
, col_042 varchar(2) NULL
, col_043 varchar(2) NULL
, col_044 varchar(2) NULL
, col_045 varchar(2) NULL
, col_046 varchar(2) NULL
, col_047 varchar(2) NULL
, col_048 varchar(2) NULL
, col_049 varchar(2) NULL
, col_050 varchar(2) NULL
, col_051 decimal(1) NULL
, col_052 decimal(1) NULL
, col_053 decimal(1) NULL
, col_054 decimal(1) NULL
, col_055 decimal(1) NULL
, col_056 decimal(1) NULL
, col_057 decimal(1) NULL
, col_058 decimal(1) NULL
, col_059 decimal(1) NULL
, col_060 decimal(1) NULL
, col_061 varchar(2) NULL
, col_062 varchar(2) NULL
, col_063 varchar(2) NULL
, col_064 varchar(2) NULL
, col_065 varchar(2) NULL
, col_066 varchar(2) NULL
, col_067 varchar(2) NULL
, col_068 varchar(2) NULL
, col_069 varchar(2) NULL
, col_070 varchar(2) NULL
, col_071 decimal(1) NULL
, col_072 decimal(1) NULL
, col_073 decimal(1) NULL
, col_074 decimal(1) NULL
, col_075 decimal(1) NULL
, col_076 decimal(1) NULL
, col_077 decimal(1) NULL
, col_078 decimal(1) NULL
, col_079 decimal(1) NULL
, col_080 decimal(1) NULL
, col_081 varchar(2) NULL
, col_082 varchar(2) NULL
, col_083 varchar(2) NULL
, col_084 varchar(2) NULL
, col_085 varchar(2) NULL
, col_086 varchar(2) NULL
, col_087 varchar(2) NULL
, col_088 varchar(2) NULL
, col_089 varchar(2) NULL
, col_090 varchar(2) NULL
, col_091 decimal(1) NULL
, col_092 decimal(1) NULL
, col_093 decimal(1) NULL
, col_094 decimal(1) NULL
, col_095 decimal(1) NULL
, col_096 decimal(1) NULL
, col_097 decimal(1) NULL
, col_098 decimal(1) NULL
, col_099 decimal(1) NULL
, col_100 decimal(1) NULL
, col_101 varchar(2) NULL
, col_102 varchar(2) NULL
, col_103 varchar(2) NULL
, col_104 varchar(2) NULL
, col_105 varchar(2) NULL
, col_106 varchar(2) NULL
, col_107 varchar(2) NULL
, col_108 varchar(2) NULL
, col_109 varchar(2) NULL
, col_110 varchar(2) NULL
, col_111 decimal(1) NULL
, col_112 decimal(1) NULL
, col_113 decimal(1) NULL
, col_114 decimal(1) NULL
, col_115 decimal(1) NULL
, col_116 decimal(1) NULL
, col_117 decimal(1) NULL
, col_118 decimal(1) NULL
, col_119 decimal(1) NULL
, col_120 decimal(1) NULL
, col_121 varchar(2) NULL
, col_122 varchar(2) NULL
, col_123 varchar(2) NULL
, col_124 varchar(2) NULL
, col_125 varchar(2) NULL
, col_126 varchar(2) NULL
, col_127 varchar(2) NULL
, col_128 varchar(2) NULL
, col_129 varchar(2) NULL
, col_130 varchar(2) NULL
, col_131 decimal(1) NULL
, col_132 decimal(1) NULL
, col_133 decimal(1) NULL
, col_134 decimal(1) NULL
, col_135 decimal(1) NULL
, col_136 decimal(1) NULL
, col_137 decimal(1) NULL
, col_138 decimal(1) NULL
, col_139 decimal(1) NULL
, col_140 decimal(1) NULL
, col_141 varchar(2) NULL
, col_142 varchar(2) NULL
, col_143 varchar(2) NULL
, col_144 varchar(2) NULL
, col_145 varchar(2) NULL
, col_146 varchar(2) NULL
, col_147 varchar(2) NULL
, col_148 varchar(2) NULL
, col_149 varchar(2) NULL
, col_150 varchar(2) NULL
, col_151 decimal(1) NULL
, col_152 decimal(1) NULL
, col_153 decimal(1) NULL
, col_154 decimal(1) NULL
, col_155 decimal(1) NULL
, col_156 decimal(1) NULL
, col_157 decimal(1) NULL
, col_158 decimal(1) NULL
, col_159 decimal(1) NULL
, col_160 decimal(1) NULL
, col_161 varchar(2) NULL
, col_162 varchar(2) NULL
, col_163 varchar(2) NULL
, col_164 varchar(2) NULL
, col_165 varchar(2) NULL
, col_166 varchar(2) NULL
, col_167 varchar(2) NULL
, col_168 varchar(2) NULL
, col_169 varchar(2) NULL
, col_170 varchar(2) NULL
, col_171 decimal(1) NULL
, col_172 decimal(1) NULL
, col_173 decimal(1) NULL
, col_174 decimal(1) NULL
, col_175 decimal(1) NULL
, col_176 decimal(1) NULL
, col_177 decimal(1) NULL
, col_178 decimal(1) NULL
, col_179 decimal(1) NULL
, col_180 decimal(1) NULL
, col_181 varchar(2) NULL
, col_182 varchar(2) NULL
, col_183 varchar(2) NULL
, col_184 varchar(2) NULL
, col_185 varchar(2) NULL
, col_186 varchar(2) NULL
, col_187 varchar(2) NULL
, col_188 varchar(2) NULL
, col_189 varchar(2) NULL
, col_190 varchar(2) NULL
, col_191 decimal(1) NULL
, col_192 decimal(1) NULL
, col_193 decimal(1) NULL
, col_194 decimal(1) NULL
, col_195 decimal(1) NULL
, col_196 decimal(1) NULL
, col_197 decimal(1) NULL
, col_198 decimal(1) NULL
, col_199 decimal(1) NULL
, col_200 decimal(1) NULL
, col_201 varchar(2) NULL
, col_202 varchar(2) NULL
, col_203 varchar(2) NULL
, col_204 varchar(2) NULL
, col_205 varchar(2) NULL
, col_206 varchar(2) NULL
, col_207 varchar(2) NULL
, col_208 varchar(2) NULL
, col_209 varchar(2) NULL
, col_210 varchar(2) NULL
, col_211 decimal(1) NULL
, col_212 decimal(1) NULL
, col_213 decimal(1) NULL
, col_214 decimal(1) NULL
, col_215 decimal(1) NULL
, col_216 decimal(1) NULL
, col_217 decimal(1) NULL
, col_218 decimal(1) NULL
, col_219 decimal(1) NULL
, col_220 decimal(1) NULL
, col_221 varchar(2) NULL
, col_222 varchar(2) NULL
, col_223 varchar(2) NULL
, col_224 varchar(2) NULL
, col_225 varchar(2) NULL
, col_226 varchar(2) NULL
, col_227 varchar(2) NULL
, col_228 varchar(2) NULL
, col_229 varchar(2) NULL
, col_230 varchar(2) NULL
, col_231 decimal(1) NULL
, col_232 decimal(1) NULL
, col_233 decimal(1) NULL
, col_234 decimal(1) NULL
, col_235 decimal(1) NULL
, col_236 decimal(1) NULL
, col_237 decimal(1) NULL
, col_238 decimal(1) NULL
, col_239 decimal(1) NULL
, col_240 decimal(1) NULL
, col_241 varchar(2) NULL
, col_242 varchar(2) NULL
, col_243 varchar(2) NULL
, col_244 varchar(2) NULL
, col_245 varchar(2) NULL
, col_246 varchar(2) NULL
, col_247 varchar(2) NULL
, col_248 varchar(2) NULL
, col_249 varchar(2) NULL
, col_250 varchar(2) NULL
)
GO

INSERT INTO pso_data_validator.dvt_many_cols (id) values (1)
GO

--Integration test table used to test non-standard characters in identifiers.
DROP TABLE pso_data_validator.[dvt-identifier$_#]
GO

CREATE TABLE pso_data_validator.[dvt-identifier$_#]
(   id            int NOT NULL PRIMARY KEY
,   [col#hash]    varchar(10) NULL
,   [col$dollar]  varchar(10) NULL
,   [col-hyphen]  varchar(10) NULL
,   [col@at]      varchar(10) NULL
,   other_data    varchar(100) NULL
)
GO

INSERT INTO pso_data_validator.[dvt-identifier$_#] VALUES (1,'#','$','-','@','Row 1')
GO
INSERT INTO pso_data_validator.[dvt-identifier$_#] VALUES (2,'#','$','-','@','Row 2')
GO
INSERT INTO pso_data_validator.[dvt-identifier$_#] VALUES (3,'#','$','-','@','Row 3')
GO
INSERT INTO pso_data_validator.[dvt-identifier$_#] VALUES (4,'#','$','-','@','Row 4')
GO
INSERT INTO pso_data_validator.[dvt-identifier$_#] VALUES (5,'#','$','-','@','Row 5')
GO

--Integration test table used to test potentially difficult timestamps.
DROP TABLE pso_data_validator.dvt_tricky_dates
GO
CREATE TABLE pso_data_validator.dvt_tricky_dates (
  id            integer NOT NULL PRIMARY KEY
, col_dt_low    date NULL
, col_dt_epoch  date NULL
, col_dt_high   date NULL
, col_dt_4712   date NULL
, col_ts_low    bigdatetime NULL
, col_ts_epoch  bigdatetime NULL
, col_ts_high   bigdatetime NULL
, col_ts_4712   bigdatetime NULL)
GO
INSERT INTO pso_data_validator.dvt_tricky_dates VALUES
(1,'1000-01-01','1970-01-01','9999-12-31','4712-12-31'
,'1000-01-01 00:00:00','1970-01-01 00:00:00','9999-12-31 23:59:59','4712-12-31 23:23:59')
GO
INSERT INTO pso_data_validator.dvt_tricky_dates (id) VALUES (2)
GO

--Integration test table used to test potentially difficult strings.
DROP TABLE pso_data_validator.dvt_tricky_strings
GO
CREATE TABLE pso_data_validator.dvt_tricky_strings (
  id           integer NOT NULL PRIMARY KEY
, col_string   varchar(20) NULL
, col_comment  varchar(40) NULL)
GO
INSERT INTO pso_data_validator.dvt_tricky_strings VALUES (1,'str'+CHAR(10)+'str','Contains: new line')
GO
INSERT INTO pso_data_validator.dvt_tricky_strings VALUES (2,'str'+CHAR(10),'Trailing: new line')
GO
INSERT INTO pso_data_validator.dvt_tricky_strings VALUES (3,'str'+CHAR(13)+'str','Contains: carriage return')
GO
INSERT INTO pso_data_validator.dvt_tricky_strings VALUES (4,'str'+CHAR(13),'Trailing: carriage return')
GO
INSERT INTO pso_data_validator.dvt_tricky_strings VALUES (5,'str'+CHAR(9)+'str','Contains: tab')
GO
INSERT INTO pso_data_validator.dvt_tricky_strings VALUES (6,'str'+CHAR(9),'Trailing: tab')
GO

--Integration test table used to test potentially difficult column names.
DROP TABLE pso_data_validator.dvt_reserved_word_columns
GO
CREATE TABLE pso_data_validator.dvt_reserved_word_columns (
  id         integer NOT NULL PRIMARY KEY
-- SQL tokens
, [select]   varchar(10) NULL
, [column]   varchar(10) NULL
, [from]     varchar(10) NULL
, [where]    varchar(10) NULL
-- Data types
, [date]     varchar(10) NULL
, [number]   varchar(10) NULL
, [string]   varchar(10) NULL
)
GO
INSERT INTO pso_data_validator.dvt_reserved_word_columns (id) VALUES (1)
GO

--Table for testing generate table partitions, consists of 32 rows with a composite primary key. Quoted Strings are handled correctly.
DROP TABLE pso_data_validator.test_generate_partitions_v2
GO

CREATE TABLE pso_data_validator.test_generate_partitions_v2
(   course_id           varchar(24)
,   quarter_id          int
,   recd_timestamp      datetime
,   registration_date   date
,   approved            bit
,   grade               decimal(5,2)
)
GO

INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG001', 1234, '2023-08-26 16:00:00', '1969-07-20', 1, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG001', 1234, '2023-08-26 16:00:00', '1969-07-20', 0, 2.8)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG001', 5678, '2023-08-26 16:00:00', '2023-08-23', 1, 2.1)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG001', 5678, '2023-08-26 16:00:00', '2023-08-23', 0, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG003', 1234, '2023-08-27 15:00:00', '1969-07-20', 1, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG003', 1234, '2023-08-27 15:00:00', '1969-07-20', 0, 2.8)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG003', 5678, '2023-08-27 15:00:00', '2023-08-23', 1, 2.1)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG003', 5678, '2023-08-27 15:00:00', '2023-08-23', 0, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG002', 1234, '2023-08-26 16:00:00', '1969-07-20', 1, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG002', 1234, '2023-08-26 16:00:00', '1969-07-20', 0, 2.8)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG002  t0.', 5678, '2023-08-26 16:00:00', '2023-08-23', 1, 2.1)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG002', 5678, '2023-08-26 16:00:00', '2023-08-23', 0, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG004', 1234, '2023-08-27 15:00:00', '1969-07-20', 1, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG004', 1234, '2023-08-27 15:00:00', '1969-07-20', 0, 2.8)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG004', 5678, '2023-08-27 15:00:00', '2023-08-23', 1, 2.1)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('ALG004', 5678, '2023-08-27 15:00:00', '2023-08-23', 0, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. John''s', 1234, '2023-08-26 16:00:00', '1969-07-20', 1, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. John''s', 1234, '2023-08-26 16:00:00', '1969-07-20', 0, 2.8)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. John''s', 5678, '2023-08-26 16:00:00', '2023-08-23', 1, 2.1)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. John''s', 5678, '2023-08-26 16:00:00', '2023-08-23', 0, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. Jude''s', 1234, '2023-08-27 15:00:00', '1969-07-20', 1, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. Jude''s', 1234, '2023-08-27 15:00:00', '1969-07-20', 0, 2.8)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. Jude''s', 5678, '2023-08-27 15:00:00', '2023-08-23', 1, 2.1)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. Jude''s', 5678, '2023-08-27 15:00:00', '2023-08-23', 0, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. Edward''s', 1234, '2023-08-26 16:00:00', '1969-07-20', 1, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. Edward''s', 1234, '2023-08-26 16:00:00', '1969-07-20', 0, 2.8)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. Edward''s', 5678, '2023-08-26 16:00:00', '2023-08-23', 1, 2.1)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. Edward''s', 5678, '2023-08-26 16:00:00', '2023-08-23', 0, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. Paul''s', 1234, '2023-08-27 15:00:00', '1969-07-20', 1, 3.5)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. Paul''s', 1234, '2023-08-27 15:00:00', '1969-07-20', 0, 2.8)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. Paul''s', 5678, '2023-08-27 15:00:00', '2023-08-23', 1, 2.1)
GO
INSERT INTO pso_data_validator.test_generate_partitions_v2 VALUES ('St. Paul''s', 5678, '2023-08-27 15:00:00', '2023-08-23', 0, 3.5)
GO

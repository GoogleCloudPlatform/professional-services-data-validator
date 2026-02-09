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

connect to testdb

CREATE SCHEMA pso_data_validator;

-- Core data types test table, to be kept in sync with same table in other SQL engines
CREATE TABLE IF NOT EXISTS pso_data_validator.dvt_core_types
(   id              INTEGER NOT NULL PRIMARY KEY
,   col_int8        SMALLINT
,   col_int16       SMALLINT
,   col_int32       INTEGER
,   col_int64       BIGINT
,   col_dec_20      DECIMAL(20)
,   col_dec_38      DECIMAL(31)
,   col_dec_10_2    DECIMAL(10,2)
,   col_float32     REAL
,   col_float64     DOUBLE
,   col_varchar_30  VARCHAR(30)
,   col_char_2      CHARACTER(2)
,   col_string      VARCHAR(32000)
,   col_date        DATE
,   col_datetime    TIMESTAMP(3)
,   col_tstz        TIMESTAMP(3) -- Unable to create col_tstz with time zone on our test database therefore test data is adjusted.
);
COMMENT ON TABLE pso_data_validator.dvt_core_types IS 'Core data types integration test table';

INSERT INTO pso_data_validator.dvt_core_types VALUES
(1,1,1,1,1
,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1
,'Hello DVT','A ','Hello DVT'
,DATE'1970-01-01',TIMESTAMP'1970-01-01 00:00:01'
,TIMESTAMP'1970-01-01 01:00:01');
INSERT INTO pso_data_validator.dvt_core_types VALUES
(2,2,2,2,2
,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2
,'Hello DVT','B','Hello DVT'
,DATE'1970-01-02',TIMESTAMP'1970-01-02 00:00:02'
,TIMESTAMP'1970-01-02 02:00:02');
INSERT INTO pso_data_validator.dvt_core_types VALUES
(3,3,3,3,3
,12345678901234567890,1234567890123456789012345,123.3,123456.3,12345678.3
,'Hello DVT','C ','Hello DVT'
,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:03'
,TIMESTAMP'1970-01-03 03:00:03');
COMMIT;

-- Db2 data types test table (data types not covered by core types).
CREATE TABLE IF NOT EXISTS pso_data_validator.dvt_db2_types
(   id              INTEGER NOT NULL PRIMARY KEY
,   col_smallint    SMALLINT
,   col_int         INTEGER
,   col_bigint      BIGINT
,   col_decfloat_16 DECFLOAT(16)
,   col_decfloat_32 DECFLOAT(34)
,   col_clob        CLOB
,   col_nvarchar_30 NVARCHAR(30)
,   col_nchar_2     NCHAR(2)
,   col_nclob       NCLOB
,   col_dbclob      DBCLOB
,   col_blob        BLOB
,   col_char_bit    CHAR(5) FOR BIT DATA
,   col_varchar_bit VARCHAR(5) FOR BIT DATA
,   col_graphic     GRAPHIC(3)
,   col_vargraphic  VARGRAPHIC(3)
,   col_time        TIME
,   col_xml         XML
);
COMMENT ON TABLE pso_data_validator.dvt_core_types IS 'Db2 data types integration test table';

INSERT INTO pso_data_validator.dvt_db2_types VALUES
(1,123,12345,1123456789,123.456,123456.789
,'Hello CLOB','Hello NVARCHAR','A ','Hello NCLOB','Hello DBCLOB'
,CAST('Hello BLOB' AS BLOB),'ABC','DEF','GHI','JKL'
,TIME'00:00:01'
,'<xml></xml>');
COMMIT;

CREATE TABLE IF NOT EXISTS pso_data_validator.dvt_null_not_null
(   col_nn             TIMESTAMP(0) NOT NULL
,   col_nullable       TIMESTAMP(0)
,   col_src_nn_trg_n   TIMESTAMP(0) NOT NULL
,   col_src_n_trg_nn   TIMESTAMP(0)
);
COMMENT ON TABLE pso_data_validator.dvt_null_not_null IS 'Nullable integration test table, DB2 is assumed to be a DVT source (not target).';

-- In Db2 the maximum precision for a DECIMAL data type is 31 digits.
-- The renders some columns in this table incompatible with the table in other systems.
CREATE TABLE IF NOT EXISTS pso_data_validator.dvt_large_decimals
(   id                DECIMAL(31) NOT NULL PRIMARY KEY
,   col_data          VARCHAR(10)
,   col_dec_18        DECIMAL(18)
,   col_dec_38        DECIMAL(31)
,   col_dec_38_9      DECIMAL(31,9)
,   col_dec_38_30     DECIMAL(31,30)
-- Columns with mismatched data for intentional fail status.
,   col_dec_18_fail   DECIMAL(18)
,   col_dec_18_1_fail DECIMAL(18,1)
);
COMMENT ON TABLE pso_data_validator.dvt_large_decimals IS 'Large decimals integration test table';

INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(123456789012345678901234567890,'Row 1'
,987654321012345678
,NULL,NULL,NULL
,987654321012345678,12345678901234567.1);
INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(223456789012345678901234567890,'Row 2'
,987654321012345678
,NULL,NULL,NULL
,987654321012345678,12345678901234567.1);
INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(323456789012345678901234567890,'Row 3'
,987654321012345678
,NULL,NULL,NULL
,987654321012345678,12345678901234567.1);
INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(423456789012345678901234567890,'Row 4'
,987654321012345678
,NULL,NULL,NULL
,987654321012345678,12345678901234567.1);
INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(523456789012345678901234567890,'Row 5'
,987654321012345678
,NULL,NULL,NULL
,987654321012345678,12345678901234567.1);
COMMIT;

CREATE TABLE IF NOT EXISTS pso_data_validator.dvt_binary
(   binary_id       VARBINARY(16) NOT NULL PRIMARY KEY
,   int_id          INTEGER NOT NULL
,   other_data      VARCHAR(100)
);
CREATE UNIQUE INDEX pso_data_validator.dvt_binary_int_id_uk ON dvt_binary (int_id);
COMMENT ON TABLE pso_data_validator.dvt_binary IS 'Integration test table used to test both binary pk matching and binary hash/concat comparisons.';
INSERT INTO pso_data_validator.dvt_binary VALUES (CAST('DVT-key-1' AS VARBINARY(16)), 1, 'Row 1');
INSERT INTO pso_data_validator.dvt_binary VALUES (CAST('DVT-key-2' AS VARBINARY(16)), 2, 'Row 2');
INSERT INTO pso_data_validator.dvt_binary VALUES (CAST('DVT-key-3' AS VARBINARY(16)), 3, 'Row 3');
INSERT INTO pso_data_validator.dvt_binary VALUES (CAST('DVT-key-4' AS VARBINARY(16)), 4, 'Row 4');
INSERT INTO pso_data_validator.dvt_binary VALUES (CAST('DVT-key-5' AS VARBINARY(16)), 5, 'Row 5');
COMMIT;

CREATE TABLE IF NOT EXISTS pso_data_validator.dvt_varchar_id
(   id          VARCHAR(15) NOT NULL PRIMARY KEY
,   other_data  VARCHAR(100)
);
COMMENT ON TABLE pso_data_validator.dvt_varchar_id IS 'Integration test table used to test varchar pk matching. Trailing blanks are significant';
INSERT INTO pso_data_validator.dvt_varchar_id VALUES ('DVT-key-1', 'Row 1');
INSERT INTO pso_data_validator.dvt_varchar_id VALUES ('DVT-key-2', 'Row 2');
INSERT INTO pso_data_validator.dvt_varchar_id VALUES ('DVT-key-3', 'Row 3');
INSERT INTO pso_data_validator.dvt_varchar_id VALUES ('DVT-key-4 ', 'Row 4');
INSERT INTO pso_data_validator.dvt_varchar_id VALUES ('DVT-key-5', 'Row 5');
COMMIT;

CREATE TABLE IF NOT EXISTS pso_data_validator.dvt_datetime_id
(   id          TIMESTAMP(0) NOT NULL PRIMARY KEY
,   other_data  VARCHAR(100)
);
COMMENT ON TABLE pso_data_validator.dvt_datetime_id IS 'Integration test table used to test datetime pk matching.';
INSERT INTO pso_data_validator.dvt_datetime_id VALUES (TIMESTAMP'2020-01-01 12:00:00', 'Row 1');
INSERT INTO pso_data_validator.dvt_datetime_id VALUES (TIMESTAMP'2020-02-01 12:00:00', 'Row 2');
INSERT INTO pso_data_validator.dvt_datetime_id VALUES (TIMESTAMP'2020-03-01 12:00:00', 'Row 3');
INSERT INTO pso_data_validator.dvt_datetime_id VALUES (TIMESTAMP'2020-04-01 12:00:00', 'Row 4');
INSERT INTO pso_data_validator.dvt_datetime_id VALUES (TIMESTAMP'2020-05-01 12:00:00', 'Row 5');
COMMIT;

CREATE TABLE IF NOT EXISTS pso_data_validator.dvt_group_by_timestamp
(   id           INTEGER NOT NULL PRIMARY KEY
,   group_id     INTEGER
,   col_date     DATE
,   col_datetime TIMESTAMP(0)
);
COMMENT ON TABLE pso_data_validator.dvt_group_by_timestamp IS 'Integration test table used to test Timestamp grouping.';
INSERT INTO pso_data_validator.dvt_group_by_timestamp VALUES (1,1,DATE'2021-01-01',TIMESTAMP'2021-01-01 12:00:00');
INSERT INTO pso_data_validator.dvt_group_by_timestamp VALUES (2,1,DATE'2021-01-01',TIMESTAMP'2021-01-01 13:00:00');
INSERT INTO pso_data_validator.dvt_group_by_timestamp VALUES (3,1,DATE'2021-01-01',TIMESTAMP'2021-01-01 14:00:00');
INSERT INTO pso_data_validator.dvt_group_by_timestamp VALUES (4,2,DATE'2022-02-02',TIMESTAMP'2022-02-02 12:00:00');
INSERT INTO pso_data_validator.dvt_group_by_timestamp VALUES (5,2,DATE'2022-02-02',TIMESTAMP'2022-02-02 13:00:00');
INSERT INTO pso_data_validator.dvt_group_by_timestamp VALUES (6,3,DATE'2023-03-03',TIMESTAMP'2023-03-03 12:00:00');
COMMIT;

CREATE TABLE IF NOT EXISTS pso_data_validator.dvt_tricky_dates (
  id            INTEGER NOT NULL PRIMARY KEY
, col_dt_low    DATE
, col_dt_epoch  DATE
, col_dt_high   DATE
, col_dt_4712   DATE
, col_ts_low    TIMESTAMP(0)
, col_ts_epoch  TIMESTAMP(0)
, col_ts_high   TIMESTAMP(0)
, col_ts_4712   TIMESTAMP(0)
);
COMMENT ON TABLE pso_data_validator.dvt_tricky_dates IS 'Integration test table used to test potentially difficult Timestamps.';
INSERT INTO pso_data_validator.dvt_tricky_dates VALUES
(1,DATE'1000-01-01',DATE'1970-01-01',DATE'9999-12-31',DATE'4712-12-31'
,TIMESTAMP'1000-01-01 00:00:00',TIMESTAMP'1970-01-01 00:00:00',TIMESTAMP'9999-12-31 23:59:59',TIMESTAMP'4712-12-31 23:23:59');
-- NULL in all columns.
INSERT INTO pso_data_validator.dvt_tricky_dates (id) VALUES (2);
COMMIT;

-- pso_data_validator.dvt_db2_generated_cols1 has fake generated columns that should be ignored.
CREATE TABLE IF NOT EXISTS pso_data_validator.dvt_db2_generated_cols1
(   id                          INTEGER NOT NULL PRIMARY KEY
,   col_int                     INTEGER
,   db2_generated_docid_for_xml INTEGER
,   db2_generated_rowid_for_lob INTEGER);
COMMENT ON TABLE pso_data_validator.dvt_db2_generated_cols1 IS 'Test table to prove generated columns are ignored.';
INSERT INTO pso_data_validator.dvt_db2_generated_cols1 VALUES (1,1,1,1);
COMMIT;
-- pso_data_validator.dvt_db2_generated_cols2 no fake generated columns, tests in tandem with dvt_db2_generated_cols1.
CREATE TABLE IF NOT EXISTS pso_data_validator.dvt_db2_generated_cols2
(   id                          INTEGER NOT NULL PRIMARY KEY
,   col_int                     INTEGER);
COMMENT ON TABLE pso_data_validator.dvt_db2_generated_cols2 IS 'Test table to prove generated columns are ignored.';
INSERT INTO pso_data_validator.dvt_db2_generated_cols2 VALUES (1,1);

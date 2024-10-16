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

-- Core data types test table, to be kept in sync with same table in other SQL engines
DROP TABLE db2inst1.dvt_core_types;
CREATE TABLE db2inst1.dvt_core_types
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
COMMENT ON TABLE db2inst1.dvt_core_types IS 'Core data types integration test table';

INSERT INTO db2inst1.dvt_core_types VALUES
(1,1,1,1,1
,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1
,'Hello DVT','A ','Hello DVT'
,DATE'1970-01-01',TIMESTAMP'1970-01-01 00:00:01'
,TIMESTAMP'1969-12-31 23:23:01');
INSERT INTO db2inst1.dvt_core_types VALUES
(2,2,2,2,2
,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2
,'Hello DVT','B','Hello DVT'
,DATE'1970-01-02',TIMESTAMP'1970-01-02 00:00:02'
,TIMESTAMP'1970-01-01 22:23:02');
INSERT INTO db2inst1.dvt_core_types VALUES
(3,3,3,3,3
,12345678901234567890,1234567890123456789012345,123.3,123456.3,12345678.3
,'Hello DVT','C ','Hello DVT'
,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:03'
,TIMESTAMP'1970-01-02 21:23:03');
COMMIT;

DROP TABLE db2inst1.dvt_null_not_null;
CREATE TABLE db2inst1.dvt_null_not_null
(   col_nn             TIMESTAMP(0) NOT NULL
,   col_nullable       TIMESTAMP(0)
,   col_src_nn_trg_n   TIMESTAMP(0) NOT NULL
,   col_src_n_trg_nn   TIMESTAMP(0)
);
COMMENT ON TABLE db2inst1.dvt_null_not_null IS 'Nullable integration test table, DB2 is assumed to be a DVT source (not target).';

DROP TABLE db2inst1.dvt_large_decimals;
CREATE TABLE db2inst1.dvt_large_decimals
(   id              DECIMAL(31) NOT NULL PRIMARY KEY
,   col_data        VARCHAR(10)
,   col_dec_18      DECIMAL(18)
,   col_dec_38      DECIMAL(31)
,   col_dec_38_9    DECIMAL(31,9)
,   col_dec_38_30   DECIMAL(31,30)
);
COMMENT ON TABLE db2inst1.dvt_large_decimals IS 'Large decimals integration test table';

/* INSERT INTO db2inst1.dvt_large_decimals VALUES
(123456789012345678901234567890
,'Row 1'
,123456789012345678
,1234567890123456789012345678901
,12345678901234567890123456789.123456
,12345678.123456789012345678901234567890);
INSERT INTO db2inst1.dvt_large_decimals VALUES
(223456789012345678901234567890
,'Row 2'
,223456789012345678
,2234567890123456789012345678901
,22345678901234567890123456789.123456789
,22345678.123456789012345678901234567890);
INSERT INTO db2inst1.dvt_large_decimals VALUES
(323456789012345678901234567890
,'Row 3'
,323456789012345678
,3234567890123456789012345678901
,32345678901234567890123456789.123456789
,32345678.123456789012345678901234567890);
COMMIT; */

DROP TABLE db2inst1.dvt_binary;
CREATE TABLE db2inst1.dvt_binary
(   binary_id       VARBINARY(16) NOT NULL PRIMARY KEY
,   int_id          INTEGER NOT NULL
,   other_data      VARCHAR(100)
);
CREATE UNIQUE INDEX db2inst1.dvt_binary_int_id_uk ON dvt_binary (int_id);
COMMENT ON TABLE db2inst1.dvt_binary IS 'Integration test table used to test both binary pk matching and binary hash/concat comparisons.';
INSERT INTO db2inst1.dvt_binary VALUES (CAST('DVT-key-1' AS VARBINARY(16)), 1, 'Row 1');
INSERT INTO db2inst1.dvt_binary VALUES (CAST('DVT-key-2' AS VARBINARY(16)), 2, 'Row 2');
INSERT INTO db2inst1.dvt_binary VALUES (CAST('DVT-key-3' AS VARBINARY(16)), 3, 'Row 3');
INSERT INTO db2inst1.dvt_binary VALUES (CAST('DVT-key-4' AS VARBINARY(16)), 4, 'Row 4');
INSERT INTO db2inst1.dvt_binary VALUES (CAST('DVT-key-5' AS VARBINARY(16)), 5, 'Row 5');
COMMIT;


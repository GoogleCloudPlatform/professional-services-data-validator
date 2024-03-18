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

DROP TABLE udf.dvt_core_types;
CREATE TABLE udf.dvt_core_types
(   id              BIGINT NOT NULL PRIMARY KEY
,   col_int8        BYTEINT
,   col_int16       SMALLINT
,   col_int32       INT
,   col_int64       BIGINT
,   col_dec_20      NUMBER(20)
,   col_dec_38      NUMBER(38)
,   col_dec_10_2    NUMBER(10,2)
,   col_float32     REAL
,   col_float64     DOUBLE PRECISION
,   col_varchar_30  VARCHAR(30)
,   col_char_2      CHAR(2)
,   col_string      LONG VARCHAR
,   col_date        DATE
,   col_datetime    TIMESTAMP(3)
,   col_tstz        TIMESTAMP(3) WITH TIME ZONE
);
COMMENT ON TABLE udf.dvt_core_types AS 'Core data types integration test table';

INSERT INTO udf.dvt_core_types VALUES
(1,1,1,1,1
,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1
,'Hello DVT','A ','Hello DVT'
,DATE'1970-01-01',TIMESTAMP'1970-01-01 00:00:01'
,CAST('1970-01-01 00:00:01.000-01:00' AS TIMESTAMP(3) WITH TIME ZONE));
INSERT INTO udf.dvt_core_types VALUES
(2,2,2,2,2
,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2
,'Hello DVT','B ','Hello DVT'
,DATE'1970-01-02',TIMESTAMP'1970-01-02 00:00:02'
,CAST('1970-01-02 00:00:02.000-02:00' AS TIMESTAMP(3) WITH TIME ZONE));
INSERT INTO udf.dvt_core_types VALUES
(3,3,3,3,3
,12345678901234567890,1234567890123456789012345,123.3,123456.3,12345678.3
,'Hello DVT','C ','Hello DVT'
,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:03'
,CAST('1970-01-03 00:00:03.000-03:00' AS TIMESTAMP(3) WITH TIME ZONE));

DROP TABLE udf.dvt_null_not_null;
CREATE TABLE udf.dvt_null_not_null
(   col_nn             TIMESTAMP(0) NOT NULL
,   col_nullable       TIMESTAMP(0)
,   col_src_nn_trg_n   TIMESTAMP(0) NOT NULL
,   col_src_n_trg_nn   TIMESTAMP(0)
);
COMMENT ON TABLE udf.dvt_null_not_null AS 'Nullable integration test table, Teradata is assumed to be a DVT source (not target).';

DROP TABLE udf.dvt_large_decimals;
CREATE TABLE udf.dvt_large_decimals
(   id              NUMBER(38) NOT NULL PRIMARY KEY
,   col_data        VARCHAR(10)
,   col_dec_18      NUMBER(18)
,   col_dec_38      NUMBER(38)
,   col_dec_38_9    NUMBER(38,9)
,   col_dec_38_30   NUMBER(38,30)
);
COMMENT ON TABLE udf.dvt_large_decimals IS 'Large decimals integration test table';

INSERT INTO udf.dvt_large_decimals VALUES
(123456789012345678901234567890
,'Row 1'
,123456789012345678
,12345678901234567890123456789012345678
,12345678901234567890123456789.123456789
,12345678.123456789012345678901234567890);
INSERT INTO udf.dvt_large_decimals VALUES
(223456789012345678901234567890
,'Row 2'
,223456789012345678
,22345678901234567890123456789012345678
,22345678901234567890123456789.123456789
,22345678.123456789012345678901234567890);
INSERT INTO udf.dvt_large_decimals VALUES
(323456789012345678901234567890
,'Row 3'
,323456789012345678
,32345678901234567890123456789012345678
,32345678901234567890123456789.123456789
,32345678.123456789012345678901234567890);

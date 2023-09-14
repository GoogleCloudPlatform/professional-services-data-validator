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

CREATE USER pso_data_validator IDENTIFIED BY pso_data_validator
DEFAULT TABLESPACE users QUOTA UNLIMITED ON users
ACCOUNT LOCK;

-- Existing Oracle integration test table
DROP TABLE pso_data_validator.items_price;
CREATE TABLE pso_data_validator.items_price
(ITEM_ID  NUMBER(10) NOT NULL
,PRICE    NUMBER
,CITY     VARCHAR2(50));
INSERT INTO pso_data_validator.items_price VALUES (2,200,'xyz');
INSERT INTO pso_data_validator.items_price VALUES (3,300,'xyz');
COMMIT;

-- Core data types test table, to be kept in sync with same table in other SQL engines
DROP TABLE pso_data_validator.dvt_core_types;
CREATE TABLE pso_data_validator.dvt_core_types
(   id              NUMBER(8) NOT NULL PRIMARY KEY
,   col_int8        NUMBER(2)
,   col_int16       NUMBER(4)
,   col_int32       NUMBER(9)
,   col_int64       NUMBER(18)
,   col_dec_20      NUMBER(20)
,   col_dec_38      NUMBER(38)
,   col_dec_10_2    NUMBER(10,2)
,   col_float32     BINARY_FLOAT
,   col_float64     BINARY_DOUBLE
,   col_varchar_30  VARCHAR(30)
,   col_char_2      CHAR(2)
,   col_string      VARCHAR(4000)
,   col_date        DATE
,   col_datetime    TIMESTAMP(3)
,   col_tstz        TIMESTAMP(3) WITH TIME ZONE
);
COMMENT ON TABLE pso_data_validator.dvt_core_types IS 'Core data types integration test table';

INSERT INTO pso_data_validator.dvt_core_types VALUES
(1,1,1,1,1
,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1
,'Hello DVT','A ','Hello DVT'
,DATE'1970-01-01',TIMESTAMP'1970-01-01 00:00:01'
,to_timestamp_tz('1970-01-01 00:00:01 -01:00','YYYY-MM-DD HH24:MI:SS TZH:TZM'));
INSERT INTO pso_data_validator.dvt_core_types VALUES
(2,2,2,2,2
,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2
,'Hello DVT','B','Hello DVT'
,DATE'1970-01-02',TIMESTAMP'1970-01-02 00:00:02'
,to_timestamp_tz('1970-01-02 00:00:02 -02:00','YYYY-MM-DD HH24:MI:SS TZH:TZM'));
INSERT INTO pso_data_validator.dvt_core_types VALUES
(3,3,3,3,3
,12345678901234567890,1234567890123456789012345,123.33,123456.3,12345678.3
,'Hello DVT','C ','Hello DVT'
,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:03'
,to_timestamp_tz('1970-01-03 00:00:03 -03:00','YYYY-MM-DD HH24:MI:SS TZH:TZM'));
COMMIT;

DROP TABLE pso_data_validator.dvt_null_not_null;
CREATE TABLE pso_data_validator.dvt_null_not_null
(   col_nn             TIMESTAMP(0) NOT NULL
,   col_nullable       TIMESTAMP(0)
,   col_src_nn_trg_n   TIMESTAMP(0) NOT NULL
,   col_src_n_trg_nn   TIMESTAMP(0)
);
COMMENT ON TABLE pso_data_validator.dvt_null_not_null IS 'Nullable integration test table, Oracle is assumed to be a DVT source (not target).';

DROP TABLE pso_data_validator.dvt_ora2pg_types;
CREATE TABLE pso_data_validator.dvt_ora2pg_types
(   id              NUMBER(8) NOT NULL PRIMARY KEY
,   col_num_4       NUMBER(4)
,   col_num_9       NUMBER(9)
,   col_num_18      NUMBER(18)
,   col_num_38      NUMBER(38)
,   col_num         NUMBER
,   col_num_10_2    NUMBER(10,2)
--,   col_num_6_m2    NUMBER(6,-2)
--,   col_num_3_5     NUMBER(3,5)
,   col_num_float   FLOAT
,   col_float32     BINARY_FLOAT
,   col_float64     BINARY_DOUBLE
,   col_varchar_30  VARCHAR2(30)
,   col_char_2      CHAR(2)
,   col_nvarchar_30 NVARCHAR2(30)
,   col_nchar_2     NCHAR(2)
,   col_date        DATE
,   col_ts          TIMESTAMP(6)
,   col_tstz        TIMESTAMP(6) WITH TIME ZONE
--,   col_tsltz       TIMESTAMP(6) WITH LOCAL TIME ZONE
,   col_raw         RAW(16)
,   col_clob        CLOB
,   col_nclob       NCLOB
);
COMMENT ON TABLE pso_data_validator.dvt_ora2pg_types IS 'Oracle to PostgreSQL integration test table';

-- Literals below match corresponding table in postgresql_test_tables.sql
INSERT INTO pso_data_validator.dvt_ora2pg_types VALUES
(1,1111,123456789,123456789012345678,1234567890123456789012345
,123.1,123.1
--,123400,0.001
,123.123,123456.1,12345678.1
,'Hello DVT','A ','Hello DVT','A '
,DATE'1970-01-01',TIMESTAMP'1970-01-01 00:00:01.123456'
,to_timestamp_tz('1970-01-01 00:00:01.123456 00:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')
--,to_timestamp_tz('1970-01-01 00:00:01.123456 00:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')
,UTL_RAW.CAST_TO_RAW('DVT'),'DVT A','DVT A'
);
INSERT INTO pso_data_validator.dvt_ora2pg_types VALUES
(2,2222,123456789,123456789012345678,1234567890123456789012345
,123.12,123.11
--,123400,0.002
,123.123,123456.1,12345678.1
,'Hello DVT','B ','Hello DVT','B '
,DATE'1970-01-02',TIMESTAMP'1970-01-02 00:00:01.123456'
,to_timestamp_tz('1970-01-02 00:00:02.123456 -02:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')
--,to_timestamp_tz('1970-01-02 00:00:02.123456 -02:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')
,UTL_RAW.CAST_TO_RAW('DVT'),'DVT B','DVT B'
);
INSERT INTO pso_data_validator.dvt_ora2pg_types VALUES
(3,3333,123456789,123456789012345678,1234567890123456789012345
,123.123,123.11
--,123400,0.003
,123.123,123456.1,12345678.1
,'Hello DVT','C ','Hello DVT','C '
,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:01.123456'
,to_timestamp_tz('1970-01-03 00:00:03.123456 -03:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')
--,to_timestamp_tz('1970-01-03 00:00:03.123456 -03:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')
,UTL_RAW.CAST_TO_RAW('DVT'),'DVT C','DVT C'
);
COMMIT;

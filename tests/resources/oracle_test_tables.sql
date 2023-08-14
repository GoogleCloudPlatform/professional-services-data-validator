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

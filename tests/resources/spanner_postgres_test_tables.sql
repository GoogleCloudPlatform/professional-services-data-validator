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

CREATE SCHEMA pso_data_validator;

DROP TABLE IF EXISTS pso_data_validator.dvt_core_types;
CREATE TABLE pso_data_validator.dvt_core_types
(   id              int NOT NULL PRIMARY KEY
,   col_int8        int
,   col_int16       int
,   col_int32       int
,   col_int64       bigint
,   col_dec_20      decimal
,   col_dec_38      decimal
,   col_dec_10_2    decimal
,   col_float32     real
,   col_float64     double precision
,   col_varchar_30  varchar(30)
,   col_char_2      varchar(2)
,   col_string      text
,   col_date        date
,   col_datetime    timestamptz
,   col_tstz        timestamptz
);

INSERT INTO pso_data_validator.dvt_core_types VALUES
(1,1,1,1,1
 ,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1
 ,'Hello DVT','A ','Hello DVT'
 ,DATE'1970-01-01',TIMESTAMP WITH TIME ZONE'1970-01-01 00:00:01 +00:00'
 ,TIMESTAMP WITH TIME ZONE'1970-01-01 00:00:01 -01:00'),
(2,2,2,2,2
 ,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2
 ,'Hello DVT','B','Hello DVT'
 ,DATE'1970-01-02',TIMESTAMP WITH TIME ZONE'1970-01-02 00:00:02 +00:00'
 ,TIMESTAMP WITH TIME ZONE'1970-01-02 00:00:02 -02:00'),
(3,3,3,3,3
 ,12345678901234567890,1234567890123456789012345,123.3,123456.3,12345678.3
 ,'Hello DVT','C ','Hello DVT'
 ,DATE'1970-01-03',TIMESTAMP WITH TIME ZONE'1970-01-03 00:00:03 +00:00'
 ,TIMESTAMP WITH TIME ZONE'1970-01-03 00:00:03 -03:00');

DROP TABLE IF EXISTS pso_data_validator.dvt_spg_types;
CREATE TABLE pso_data_validator.dvt_spg_types
(   id              bigint NOT NULL PRIMARY KEY
,   col_int8        bigint
,   col_dec         decimal
,   col_dec_10_2    decimal
,   col_float32     real
,   col_float64     double precision
,   col_varchar_30  varchar(30)
,   col_text        text
,   col_date        date
,   col_ts          timestamptz
,   col_tstz        timestamptz
,   col_binary      bytea
,   col_bool        boolean
,   col_jsonb       jsonb
,   col_uuid        uuid
);

INSERT INTO pso_data_validator.dvt_spg_types
(id,col_int8,col_dec,col_dec_10_2,col_float32,col_float64
,col_varchar_30,col_text,col_date,col_ts,col_tstz
,col_binary,col_bool,col_jsonb,col_uuid)
VALUES
(1,123456789012345678,12345678901234567890.12345,123.12
,123456.1,12345678.1
,'Hello DVT','Hello DVT'
,DATE'1970-01-01',TIMESTAMP WITH TIME ZONE'1970-01-01 00:00:01.123456 +00:00'
,TIMESTAMP WITH TIME ZONE'1970-01-01 00:00:01.123456 +00:00'
,CAST('DVT' AS BYTEA),TRUE
,JSONB'[1,2,3]',gen_random_uuid())
,(2,223456789012345678,22345678901234567890.12345,223.12
,223456.1,22345678.1
,'Hello DVT','Hello DVT'
,DATE'1970-01-02',TIMESTAMP WITH TIME ZONE'1970-01-02 00:00:02.123456 +00:00'
,TIMESTAMP WITH TIME ZONE'1970-01-02 00:00:02.123456 +00:00'
,CAST('DVT' AS BYTEA),FALSE
,JSONB'[1,2,3]',gen_random_uuid())
,(3,NULL,NULL,NULL,NULL,NULL
,NULL,NULL,NULL,NULL,NULL
,NULL,NULL,NULL,NULL);

DROP TABLE IF EXISTS pso_data_validator.dvt_null_not_null;
CREATE TABLE pso_data_validator.dvt_null_not_null
(   col_nn             timestamptz NOT NULL PRIMARY KEY
,   col_nullable       timestamptz
,   col_src_nn_trg_n   timestamptz NOT NULL
,   col_src_n_trg_nn   timestamptz
);

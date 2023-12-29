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

CREATE SCHEMA pso_data_validator;
DROP TABLE pso_data_validator.dvt_core_types;
CREATE TABLE pso_data_validator.dvt_core_types
(   id              int NOT NULL PRIMARY KEY
,   col_int8        smallint
,   col_int16       smallint
,   col_int32       int
,   col_int64       bigint
,   col_dec_20      decimal(20)
,   col_dec_38      decimal(38)
,   col_dec_10_2    decimal(10,2)
,   col_float32     real
,   col_float64     double precision
,   col_varchar_30  varchar(30)
,   col_char_2      char(2)
,   col_string      text
,   col_date        date
,   col_datetime    timestamp(3)
,   col_tstz        timestamp(3) with time zone
);
COMMENT ON TABLE pso_data_validator.dvt_core_types IS 'Core data types integration test table';

INSERT INTO pso_data_validator.dvt_core_types VALUES
(1,1,1,1,1
 ,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1
 ,'Hello DVT','A ','Hello DVT'
 ,DATE'1970-01-01',TIMESTAMP'1970-01-01 00:00:01'
 ,TIMESTAMP WITH TIME ZONE'1970-01-01 00:00:01 -01:00'),
(2,2,2,2,2
 ,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2
 ,'Hello DVT','B','Hello DVT'
 ,DATE'1970-01-02',TIMESTAMP'1970-01-02 00:00:02'
 ,TIMESTAMP WITH TIME ZONE'1970-01-02 00:00:02 -02:00'),
(3,3,3,3,3
 ,12345678901234567890,1234567890123456789012345,123.33,123456.3,12345678.3
 ,'Hello DVT','C ','Hello DVT'
 ,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:03'
 ,TIMESTAMP WITH TIME ZONE'1970-01-03 00:00:03 -03:00');

DROP TABLE pso_data_validator.dvt_ora2pg_types;
CREATE TABLE pso_data_validator.dvt_ora2pg_types
(   id              int NOT NULL PRIMARY KEY
,   col_num_4       smallint
,   col_num_9       int
,   col_num_18      bigint
,   col_num_38      decimal(38)
,   col_num         decimal
,   col_num_10_2    decimal(10,2)
--,   col_num_6_m2    decimal(6,0)
--,   col_num_3_5     decimal(5,5)
,   col_num_float   decimal
,   col_float32     real
,   col_float64     double precision
,   col_varchar_30  varchar(30)
,   col_char_2      char(2)
,   col_nvarchar_30 varchar(30)
,   col_nchar_2     char(2)
,   col_date        date
,   col_ts          timestamp(6)
,   col_tstz        timestamp(6) with time zone
--,   col_tsltz       timestamp(6) with time zone
,   col_raw         bytea
,   col_blob        bytea
,   col_clob        text
,   col_nclob       text
);
COMMENT ON TABLE pso_data_validator.dvt_ora2pg_types IS 'Oracle to PostgreSQL integration test table';

-- Literals below match corresponding table in oracle_test_tables.sql
INSERT INTO pso_data_validator.dvt_ora2pg_types VALUES
(1,1111,123456789,123456789012345678,1234567890123456789012345
,123.1,123.1
--,123400,0.001
,123.123,123456.1,12345678.1
,'Hello DVT','A ','Hello DVT','A '
,DATE'1970-01-01',TIMESTAMP'1970-01-01 00:00:01.123456'
,TIMESTAMP WITH TIME ZONE'1970-01-01 00:00:01.123456 +00:00'
--,TIMESTAMP WITH TIME ZONE'1970-01-01 00:00:01.123456 +00:00'
,CAST('DVT' AS BYTEA),CAST('DVT' AS BYTEA),'DVT A','DVT A')
,(2,2222,123456789,123456789012345678,1234567890123456789012345
,123.12,123.11
--,123400,0.002
,123.123,123456.1,12345678.1
,'Hello DVT','B ','Hello DVT','B '
,DATE'1970-01-02',TIMESTAMP'1970-01-02 00:00:01.123456'
,TIMESTAMP WITH TIME ZONE'1970-01-02 00:00:02.123456 -02:00'
--,TIMESTAMP WITH TIME ZONE'1970-01-02 00:00:02.123456 -02:00'
,CAST('DVT' AS BYTEA),CAST('DVT DVT' AS BYTEA),'DVT B','DVT B')
,(3,3333,123456789,123456789012345678,1234567890123456789012345
,123.123,123.11
--,123400,0.003
,123.123,123456.1,12345678.1
,'Hello DVT','C ','Hello DVT','C '
,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:01.123456'
,TIMESTAMP WITH TIME ZONE'1970-01-03 00:00:03.123456 -03:00'
--,TIMESTAMP WITH TIME ZONE'1970-01-03 00:00:03.123456 -03:00'
,CAST('DVT' AS BYTEA),CAST('DVT DVT DVT' AS BYTEA),'DVT C','DVT C'
);

 /* Following table used for validating generating table partitions */
\c guestbook
drop table if exists public.test_generate_partitions ;
CREATE TABLE public.test_generate_partitions (
        course_id VARCHAR(12),
        quarter_id INTEGER,
        recd_timestamp TIMESTAMP,
        registration_date DATE,
        approved Boolean,
        grade NUMERIC,
        PRIMARY KEY (course_id, quarter_id, recd_timestamp, registration_date, approved));
COMMENT ON TABLE public.test_generate_partitions IS 'Table for testing generate table partitions, consists of 32 rows with a composite primary key';

INSERT INTO public.test_generate_partitions (course_id, quarter_id, recd_timestamp, registration_date, approved, grade) VALUES
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
        ('ALG002', 5678, '2023-08-26 4:00pm', '2023-08-23', True, 2.1),
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

DROP TABLE pso_data_validator.dvt_null_not_null;
CREATE TABLE pso_data_validator.dvt_null_not_null
(   col_nn             TIMESTAMP(0) NOT NULL
,   col_nullable       TIMESTAMP(0)
,   col_src_nn_trg_n   TIMESTAMP(0) NOT NULL
,   col_src_n_trg_nn   TIMESTAMP(0)
);
COMMENT ON TABLE pso_data_validator.dvt_null_not_null IS 'Nullable integration test table, PostgreSQL is assumed to be a DVT source (not target).';

DROP TABLE pso_data_validator.dvt_pg_types;
CREATE TABLE pso_data_validator.dvt_pg_types
(   id              serial NOT NULL PRIMARY KEY
,   col_int2        smallint
,   col_int4        int
,   col_int8        bigint
,   col_dec         decimal
,   col_dec_10_2    decimal(10,2)
--,   col_money       money
,   col_float32     real
,   col_float64     double precision
,   col_varchar_30  varchar(30)
,   col_char_2      char(2)
,   col_text        text
,   col_date        date
,   col_ts          timestamp(6) without time zone
,   col_tstz        timestamp(6) with time zone
,   col_time        time(6) without time zone
,   col_timetz      time(6) with time zone
,   col_binary      bytea
,   col_bool        boolean
--,   col_bit         bit(3)
--,   col_bitv        bit varying(3)
,   col_uuid        uuid
,   col_oid         oid
);
COMMENT ON TABLE pso_data_validator.dvt_pg_types IS 'PostgreSQL data types integration test table';

INSERT INTO pso_data_validator.dvt_pg_types
(col_int2,col_int4,col_int8,col_dec,col_dec_10_2
--,col_money
,col_float32,col_float64
,col_varchar_30,col_char_2,col_text
,col_date,col_ts,col_tstz,col_time,col_timetz
,col_binary,col_bool
--,col_bit,col_bitv
,col_uuid,col_oid)
VALUES
(1111,123456789,123456789012345678,12345678901234567890.12345,123.12
--,123.12
,123456.1,12345678.1
,'Hello DVT','A ','Hello DVT'
,DATE'1970-01-01',TIMESTAMP'1970-01-01 00:00:01.123456'
,TIMESTAMP WITH TIME ZONE'1970-01-01 00:00:01.123456 +00:00'
,TIME'00:00:01.123456',TIME WITH TIME ZONE'00:00:01.123456 +00:00'
,CAST('DVT' AS BYTEA),CAST(0 AS BOOLEAN)
--,B'101', B'101'
,gen_random_uuid(),1)
,(2222,223456789,223456789012345678,22345678901234567890.12345,223.12
--,223.12
,223456.1,22345678.1
,'Hello DVT','B ','Hello DVT'
,DATE'1970-01-02',TIMESTAMP'1970-01-02 00:00:02.123456'
,TIMESTAMP WITH TIME ZONE'1970-01-02 00:00:02.123456 +00:00'
,TIME'00:00:02.123456',TIME WITH TIME ZONE'00:00:02.123456 +00:00'
,CAST('DVT' AS BYTEA),CAST(0 AS BOOLEAN)
--,B'011', B'110'
,gen_random_uuid(),2);

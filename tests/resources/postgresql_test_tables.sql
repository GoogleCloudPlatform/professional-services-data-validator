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
--,   col_raw         bytea
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
--,CAST('DVT' AS BYTEA)
,'DVT A','DVT A'
);
INSERT INTO pso_data_validator.dvt_ora2pg_types VALUES
(2,2222,123456789,123456789012345678,1234567890123456789012345
,123.12,123.11
--,123400,0.002
,123.123,123456.1,12345678.1
,'Hello DVT','B ','Hello DVT','B '
,DATE'1970-01-02',TIMESTAMP'1970-01-02 00:00:01.123456'
,TIMESTAMP WITH TIME ZONE'1970-01-02 00:00:02.123456 -02:00'
--,TIMESTAMP WITH TIME ZONE'1970-01-02 00:00:02.123456 -02:00'
--,CAST('DVT' AS BYTEA)
,'DVT B','DVT B'
);
INSERT INTO pso_data_validator.dvt_ora2pg_types VALUES
(3,3333,123456789,123456789012345678,1234567890123456789012345
,123.123,123.11
--,123400,0.003
,123.123,123456.1,12345678.1
,'Hello DVT','C ','Hello DVT','C '
,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:01.123456'
,TIMESTAMP WITH TIME ZONE'1970-01-03 00:00:03.123456 -03:00'
--,TIMESTAMP WITH TIME ZONE'1970-01-03 00:00:03.123456 -03:00'
--,CAST('DVT' AS BYTEA)
,'DVT C','DVT C'
);

 /* Following table used for validating generating table partitions */
\c guestbook
drop table if exists public.test_generate_partitions ;
CREATE TABLE public.test_generate_partitions (
        course_id VARCHAR(6),
        quarter_id INTEGER,
        student_id INTEGER,
        grade NUMERIC,
        PRIMARY KEY (course_id, quarter_id, student_id));
COMMENT ON TABLE public.test_generate_partitions IS 'Table for testing generate table partitions, consists of 27 rows with course_id, quarter_id, student_id as a composite primary key';

INSERT INTO public.test_generate_partitions (course_id, quarter_id, student_id, grade) VALUES
        ('ALG001', 1, 1234, 2.1),
        ('ALG001', 1, 5678, 3.5),
        ('ALG001', 1, 9012, 2.3),
        ('ALG001', 2, 1234, 3.5),
        ('ALG001', 2, 5678, 2.6),
        ('ALG001', 2, 9012, 3.5),
        ('ALG001', 3, 1234, 2.7),
        ('ALG001', 3, 5678, 3.5),
        ('ALG001', 3, 9012, 2.8),
        ('GEO001', 1, 1234, 2.1),
        ('GEO001', 1, 5678, 3.5),
        ('GEO001', 1, 9012, 2.3),
        ('GEO001', 2, 1234, 3.5),
        ('GEO001', 2, 5678, 2.6),
        ('GEO001', 2, 9012, 3.5),
        ('GEO001', 3, 1234, 2.7),
        ('GEO001', 3, 5678, 3.5),
        ('GEO001', 3, 9012, 2.8),
        ('TRI001', 1, 1234, 2.1),
        ('TRI001', 1, 5678, 3.5),
        ('TRI001', 1, 9012, 2.3),
        ('TRI001', 2, 1234, 3.5),
        ('TRI001', 2, 5678, 2.6),
        ('TRI001', 2, 9012, 3.5),
        ('TRI001', 3, 1234, 2.7),
        ('TRI001', 3, 5678, 3.5),
        ('TRI001', 3, 9012, 2.8);

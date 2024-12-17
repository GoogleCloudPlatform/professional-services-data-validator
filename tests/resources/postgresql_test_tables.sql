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
DROP TABLE IF EXISTS pso_data_validator.dvt_core_types;
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
 ,12345678901234567890,1234567890123456789012345,123.3,123456.3,12345678.3
 ,'Hello DVT','C ','Hello DVT'
 ,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:03'
 ,TIMESTAMP WITH TIME ZONE'1970-01-03 00:00:03 -03:00');

DROP TABLE IF EXISTS pso_data_validator.dvt_ora2pg_types;
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
,   col_tsltz       timestamp(6) with time zone
,   col_interval_ds INTERVAL DAY TO SECOND (3)
,   col_raw         bytea
,   col_long_raw    bytea
,   col_blob        bytea
,   col_clob        text
,   col_nclob       text
,   col_json        json
,   col_jsonb       jsonb
);
COMMENT ON TABLE pso_data_validator.dvt_ora2pg_types IS 'Oracle to PostgreSQL integration test table';

-- Literals below match corresponding table in oracle_test_tables.sql
INSERT INTO pso_data_validator.dvt_ora2pg_types VALUES
(1,1111,123456789,123456789012345678,1234567890123456789012345
,0.1,123.1
--,123400,0.001
,123.123,123456.1,12345678.1
,'Hello DVT','A ','Hello DVT','A '
,DATE'1970-01-01',TIMESTAMP'1970-01-01 00:00:01.123456'
,TIMESTAMP WITH TIME ZONE'1970-01-01 00:00:01.123456 +00:00'
,TIMESTAMP WITH TIME ZONE'1970-01-01 00:00:01.123456 +00:00'
,INTERVAL '1 2:03:44.0' DAY TO SECOND(3)
,CAST('DVT' AS BYTEA),CAST('DVT' AS BYTEA)
,CAST('DVT' AS BYTEA),'DVT A','DVT A'
,'{"dvt": 123, "status": "abc"}','{"dvt": 123, "status": "abc"}')
,(2,2222,123456789,123456789012345678,1234567890123456789012345
,123.12,123.11
--,123400,0.002
,123.123,123456.1,12345678.1
,'Hello DVT','B ','Hello DVT','B '
,DATE'1970-01-02',TIMESTAMP'1970-01-02 00:00:01.123456'
,TIMESTAMP WITH TIME ZONE'1970-01-02 00:00:02.123456 -02:00'
,TIMESTAMP WITH TIME ZONE'1970-01-02 00:00:02.123456 -02:00'
,INTERVAL '2 3:04:55.666' DAY TO SECOND(3)
,CAST('DVT' AS BYTEA),CAST('DVT DVT' AS BYTEA)
,CAST('DVT DVT' AS BYTEA),'DVT B','DVT B'
,'{"dvt": 234, "status": "def"}','{"dvt": 234, "status": "def"}')
,(3,3333,123456789,123456789012345678,1234567890123456789012345
,123.123,123.11
--,123400,0.003
,123.123,123456.1,12345678.1
,'Hello DVT','C ','Hello DVT','C '
,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:01.123456'
,TIMESTAMP WITH TIME ZONE'1970-01-03 00:00:03.123456 -03:00'
,TIMESTAMP WITH TIME ZONE'1970-01-03 00:00:03.123456 -03:00'
,INTERVAL '3 4:05:06.7' DAY TO SECOND(3)
,CAST('DVT' AS BYTEA),CAST('DVT DVT DVT' AS BYTEA)
,CAST('DVT DVT DVT' AS BYTEA),'DVT C','DVT C'
,'{"dvt": 345, "status": "ghi"}','{"dvt": 345, "status": "ghi"}'
);

 /* Following table used for validating generating table partitions */
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

DROP TABLE IF EXISTS pso_data_validator.dvt_null_not_null;
CREATE TABLE pso_data_validator.dvt_null_not_null
(   col_nn             TIMESTAMP(0) NOT NULL
,   col_nullable       TIMESTAMP(0)
,   col_src_nn_trg_n   TIMESTAMP(0) NOT NULL
,   col_src_n_trg_nn   TIMESTAMP(0)
);
COMMENT ON TABLE pso_data_validator.dvt_null_not_null IS 'Nullable integration test table, PostgreSQL is assumed to be a DVT source (not target).';

DROP TABLE IF EXISTS pso_data_validator.dvt_pg_types;
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

CREATE EXTENSION pgcrypto;
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

DROP TABLE IF EXISTS pso_data_validator.dvt_large_decimals;
CREATE TABLE pso_data_validator.dvt_large_decimals
(   id              DECIMAL(38) NOT NULL PRIMARY KEY
,   col_data        VARCHAR(10)
,   col_dec_18      DECIMAL(18)
,   col_dec_38      DECIMAL(38)
,   col_dec_38_9    DECIMAL(38,9)
,   col_dec_38_30   DECIMAL(38,30)
);
COMMENT ON TABLE pso_data_validator.dvt_large_decimals IS 'Large decimals integration test table';

INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(123456789012345678901234567890
,'Row 1'
,123456789012345678
,12345678901234567890123456789012345678
,12345678901234567890123456789.123456789
,12345678.123456789012345678901234567890)
,(223456789012345678901234567890
,'Row 2'
,223456789012345678
,22345678901234567890123456789012345678
,22345678901234567890123456789.123456789
,22345678.123456789012345678901234567890)
,(323456789012345678901234567890
,'Row 3'
,323456789012345678
,32345678901234567890123456789012345678
,32345678901234567890123456789.123456789
,32345678.123456789012345678901234567890);

DROP TABLE IF EXISTS pso_data_validator.dvt_binary;
CREATE TABLE pso_data_validator.dvt_binary
(   binary_id       bytea NOT NULL PRIMARY KEY
,   int_id          int NOT NULL
,   other_data      varchar(100)
);
CREATE UNIQUE INDEX dvt_binary_int_id_uk ON pso_data_validator.dvt_binary (int_id);
COMMENT ON TABLE pso_data_validator.dvt_binary IS 'Integration test table used to test both binary pk matching and binary hash/concat comparisons.';
INSERT INTO pso_data_validator.dvt_binary VALUES
(CAST('DVT-key-1' AS bytea), 1, 'Row 1'),
(CAST('DVT-key-2' AS bytea), 2, 'Row 2'),
(CAST('DVT-key-3' AS bytea), 3, 'Row 3'),
(CAST('DVT-key-4' AS bytea), 4, 'Row 4'),
(CAST('DVT-key-5' AS bytea), 5, 'Row 5');

DROP TABLE pso_data_validator.dvt_char_id;
CREATE TABLE pso_data_validator.dvt_char_id
(   id          char(6) NOT NULL PRIMARY KEY
,   other_data  varchar(100)
);
COMMENT ON TABLE pso_data_validator.dvt_char_id IS 'Integration test table used to test CHAR pk matching.';
INSERT INTO pso_data_validator.dvt_char_id VALUES
('DVT1', 'Row 1'),
('DVT2', 'Row 2'),
('DVT3', 'Row 3'),
('DVT4', 'Row 4'),
('DVT5', 'Row 5');

DROP TABLE pso_data_validator.dvt_pangrams;
CREATE TABLE pso_data_validator.dvt_pangrams
(   id          int
,   lang        varchar(100)
,   words       varchar(1000)
,   words_en    varchar(1000)
,   CONSTRAINT dvt_pangrams_pk PRIMARY KEY (id)
);
COMMENT ON TABLE pso_data_validator.dvt_pangrams IS 'Integration test table used to test unicode characters.';
-- Text taken from Wikipedia, we cannot guarantee translations :-)
INSERT INTO pso_data_validator.dvt_pangrams VALUES
(1,'Hebrew', 'שפן אכל קצת גזר בטעם חסה, ודי',
 'A bunny ate some lettuce-flavored carrots, and he had enough'),
(2,'Polish', 'Pchnąć w tę łódź jeża lub ośm skrzyń fig',
 'Push a hedgehog or eight crates of figs in this boat'),
(3,'Russian', 'Съешь ещё этих мягких французских булок, да выпей же чаю',
 'Eat more of these soft French loaves and drink a tea'),
(4,'Swedish', 'Schweiz för lyxfjäder på qvist bakom ugn',
 'Switzerland brings luxury feather on branch behind oven'),
(5,'Turkish', 'Pijamalı hasta yağız şoföre çabucak güvendi',
 'The sick person in pyjamas quickly trusted the swarthy driver');

DROP TABLE pso_data_validator.dvt_many_cols;
CREATE TABLE pso_data_validator.dvt_many_cols
( id decimal(5)
, col_001 varchar(2)
, col_002 varchar(2)
, col_003 varchar(2)
, col_004 varchar(2)
, col_005 varchar(2)
, col_006 varchar(2)
, col_007 varchar(2)
, col_008 varchar(2)
, col_009 varchar(2)
, col_010 varchar(2)
, col_011 decimal(1)
, col_012 decimal(1)
, col_013 decimal(1)
, col_014 decimal(1)
, col_015 decimal(1)
, col_016 decimal(1)
, col_017 decimal(1)
, col_018 decimal(1)
, col_019 decimal(1)
, col_020 decimal(1)
, col_021 varchar(2)
, col_022 varchar(2)
, col_023 varchar(2)
, col_024 varchar(2)
, col_025 varchar(2)
, col_026 varchar(2)
, col_027 varchar(2)
, col_028 varchar(2)
, col_029 varchar(2)
, col_030 varchar(2)
, col_031 decimal(1)
, col_032 decimal(1)
, col_033 decimal(1)
, col_034 decimal(1)
, col_035 decimal(1)
, col_036 decimal(1)
, col_037 decimal(1)
, col_038 decimal(1)
, col_039 decimal(1)
, col_040 decimal(1)
, col_041 varchar(2)
, col_042 varchar(2)
, col_043 varchar(2)
, col_044 varchar(2)
, col_045 varchar(2)
, col_046 varchar(2)
, col_047 varchar(2)
, col_048 varchar(2)
, col_049 varchar(2)
, col_050 varchar(2)
, col_051 decimal(1)
, col_052 decimal(1)
, col_053 decimal(1)
, col_054 decimal(1)
, col_055 decimal(1)
, col_056 decimal(1)
, col_057 decimal(1)
, col_058 decimal(1)
, col_059 decimal(1)
, col_060 decimal(1)
, col_061 varchar(2)
, col_062 varchar(2)
, col_063 varchar(2)
, col_064 varchar(2)
, col_065 varchar(2)
, col_066 varchar(2)
, col_067 varchar(2)
, col_068 varchar(2)
, col_069 varchar(2)
, col_070 varchar(2)
, col_071 decimal(1)
, col_072 decimal(1)
, col_073 decimal(1)
, col_074 decimal(1)
, col_075 decimal(1)
, col_076 decimal(1)
, col_077 decimal(1)
, col_078 decimal(1)
, col_079 decimal(1)
, col_080 decimal(1)
, col_081 varchar(2)
, col_082 varchar(2)
, col_083 varchar(2)
, col_084 varchar(2)
, col_085 varchar(2)
, col_086 varchar(2)
, col_087 varchar(2)
, col_088 varchar(2)
, col_089 varchar(2)
, col_090 varchar(2)
, col_091 decimal(1)
, col_092 decimal(1)
, col_093 decimal(1)
, col_094 decimal(1)
, col_095 decimal(1)
, col_096 decimal(1)
, col_097 decimal(1)
, col_098 decimal(1)
, col_099 decimal(1)
, col_100 decimal(1)
, col_101 varchar(2)
, col_102 varchar(2)
, col_103 varchar(2)
, col_104 varchar(2)
, col_105 varchar(2)
, col_106 varchar(2)
, col_107 varchar(2)
, col_108 varchar(2)
, col_109 varchar(2)
, col_110 varchar(2)
, col_111 decimal(1)
, col_112 decimal(1)
, col_113 decimal(1)
, col_114 decimal(1)
, col_115 decimal(1)
, col_116 decimal(1)
, col_117 decimal(1)
, col_118 decimal(1)
, col_119 decimal(1)
, col_120 decimal(1)
, col_121 varchar(2)
, col_122 varchar(2)
, col_123 varchar(2)
, col_124 varchar(2)
, col_125 varchar(2)
, col_126 varchar(2)
, col_127 varchar(2)
, col_128 varchar(2)
, col_129 varchar(2)
, col_130 varchar(2)
, col_131 decimal(1)
, col_132 decimal(1)
, col_133 decimal(1)
, col_134 decimal(1)
, col_135 decimal(1)
, col_136 decimal(1)
, col_137 decimal(1)
, col_138 decimal(1)
, col_139 decimal(1)
, col_140 decimal(1)
, col_141 varchar(2)
, col_142 varchar(2)
, col_143 varchar(2)
, col_144 varchar(2)
, col_145 varchar(2)
, col_146 varchar(2)
, col_147 varchar(2)
, col_148 varchar(2)
, col_149 varchar(2)
, col_150 varchar(2)
, col_151 decimal(1)
, col_152 decimal(1)
, col_153 decimal(1)
, col_154 decimal(1)
, col_155 decimal(1)
, col_156 decimal(1)
, col_157 decimal(1)
, col_158 decimal(1)
, col_159 decimal(1)
, col_160 decimal(1)
, col_161 varchar(2)
, col_162 varchar(2)
, col_163 varchar(2)
, col_164 varchar(2)
, col_165 varchar(2)
, col_166 varchar(2)
, col_167 varchar(2)
, col_168 varchar(2)
, col_169 varchar(2)
, col_170 varchar(2)
, col_171 decimal(1)
, col_172 decimal(1)
, col_173 decimal(1)
, col_174 decimal(1)
, col_175 decimal(1)
, col_176 decimal(1)
, col_177 decimal(1)
, col_178 decimal(1)
, col_179 decimal(1)
, col_180 decimal(1)
, col_181 varchar(2)
, col_182 varchar(2)
, col_183 varchar(2)
, col_184 varchar(2)
, col_185 varchar(2)
, col_186 varchar(2)
, col_187 varchar(2)
, col_188 varchar(2)
, col_189 varchar(2)
, col_190 varchar(2)
, col_191 decimal(1)
, col_192 decimal(1)
, col_193 decimal(1)
, col_194 decimal(1)
, col_195 decimal(1)
, col_196 decimal(1)
, col_197 decimal(1)
, col_198 decimal(1)
, col_199 decimal(1)
, col_200 decimal(1)
, col_201 varchar(2)
, col_202 varchar(2)
, col_203 varchar(2)
, col_204 varchar(2)
, col_205 varchar(2)
, col_206 varchar(2)
, col_207 varchar(2)
, col_208 varchar(2)
, col_209 varchar(2)
, col_210 varchar(2)
, col_211 decimal(1)
, col_212 decimal(1)
, col_213 decimal(1)
, col_214 decimal(1)
, col_215 decimal(1)
, col_216 decimal(1)
, col_217 decimal(1)
, col_218 decimal(1)
, col_219 decimal(1)
, col_220 decimal(1)
, col_221 varchar(2)
, col_222 varchar(2)
, col_223 varchar(2)
, col_224 varchar(2)
, col_225 varchar(2)
, col_226 varchar(2)
, col_227 varchar(2)
, col_228 varchar(2)
, col_229 varchar(2)
, col_230 varchar(2)
, col_231 decimal(1)
, col_232 decimal(1)
, col_233 decimal(1)
, col_234 decimal(1)
, col_235 decimal(1)
, col_236 decimal(1)
, col_237 decimal(1)
, col_238 decimal(1)
, col_239 decimal(1)
, col_240 decimal(1)
, col_241 varchar(2)
, col_242 varchar(2)
, col_243 varchar(2)
, col_244 varchar(2)
, col_245 varchar(2)
, col_246 varchar(2)
, col_247 varchar(2)
, col_248 varchar(2)
, col_249 varchar(2)
, col_250 varchar(2)
, col_251 decimal(1)
, col_252 decimal(1)
, col_253 decimal(1)
, col_254 decimal(1)
, col_255 decimal(1)
, col_256 decimal(1)
, col_257 decimal(1)
, col_258 decimal(1)
, col_259 decimal(1)
, col_260 decimal(1)
, col_261 varchar(2)
, col_262 varchar(2)
, col_263 varchar(2)
, col_264 varchar(2)
, col_265 varchar(2)
, col_266 varchar(2)
, col_267 varchar(2)
, col_268 varchar(2)
, col_269 varchar(2)
, col_270 varchar(2)
, col_271 decimal(1)
, col_272 decimal(1)
, col_273 decimal(1)
, col_274 decimal(1)
, col_275 decimal(1)
, col_276 decimal(1)
, col_277 decimal(1)
, col_278 decimal(1)
, col_279 decimal(1)
, col_280 decimal(1)
, col_281 varchar(2)
, col_282 varchar(2)
, col_283 varchar(2)
, col_284 varchar(2)
, col_285 varchar(2)
, col_286 varchar(2)
, col_287 varchar(2)
, col_288 varchar(2)
, col_289 varchar(2)
, col_290 varchar(2)
, col_291 decimal(1)
, col_292 decimal(1)
, col_293 decimal(1)
, col_294 decimal(1)
, col_295 decimal(1)
, col_296 decimal(1)
, col_297 decimal(1)
, col_298 decimal(1)
, col_299 decimal(1)
, col_300 decimal(1)
, col_301 varchar(2)
, col_302 varchar(2)
, col_303 varchar(2)
, col_304 varchar(2)
, col_305 varchar(2)
, col_306 varchar(2)
, col_307 varchar(2)
, col_308 varchar(2)
, col_309 varchar(2)
, col_310 varchar(2)
, col_311 decimal(1)
, col_312 decimal(1)
, col_313 decimal(1)
, col_314 decimal(1)
, col_315 decimal(1)
, col_316 decimal(1)
, col_317 decimal(1)
, col_318 decimal(1)
, col_319 decimal(1)
, col_320 decimal(1)
, col_321 varchar(2)
, col_322 varchar(2)
, col_323 varchar(2)
, col_324 varchar(2)
, col_325 varchar(2)
, col_326 varchar(2)
, col_327 varchar(2)
, col_328 varchar(2)
, col_329 varchar(2)
, col_330 varchar(2)
, col_331 decimal(1)
, col_332 decimal(1)
, col_333 decimal(1)
, col_334 decimal(1)
, col_335 decimal(1)
, col_336 decimal(1)
, col_337 decimal(1)
, col_338 decimal(1)
, col_339 decimal(1)
, col_340 decimal(1)
, col_341 varchar(2)
, col_342 varchar(2)
, col_343 varchar(2)
, col_344 varchar(2)
, col_345 varchar(2)
, col_346 varchar(2)
, col_347 varchar(2)
, col_348 varchar(2)
, col_349 varchar(2)
, col_350 varchar(2)
, col_351 decimal(1)
, col_352 decimal(1)
, col_353 decimal(1)
, col_354 decimal(1)
, col_355 decimal(1)
, col_356 decimal(1)
, col_357 decimal(1)
, col_358 decimal(1)
, col_359 decimal(1)
, col_360 decimal(1)
, col_361 varchar(2)
, col_362 varchar(2)
, col_363 varchar(2)
, col_364 varchar(2)
, col_365 varchar(2)
, col_366 varchar(2)
, col_367 varchar(2)
, col_368 varchar(2)
, col_369 varchar(2)
, col_370 varchar(2)
, col_371 decimal(1)
, col_372 decimal(1)
, col_373 decimal(1)
, col_374 decimal(1)
, col_375 decimal(1)
, col_376 decimal(1)
, col_377 decimal(1)
, col_378 decimal(1)
, col_379 decimal(1)
, col_380 decimal(1)
, col_381 varchar(2)
, col_382 varchar(2)
, col_383 varchar(2)
, col_384 varchar(2)
, col_385 varchar(2)
, col_386 varchar(2)
, col_387 varchar(2)
, col_388 varchar(2)
, col_389 varchar(2)
, col_390 varchar(2)
, col_391 decimal(1)
, col_392 decimal(1)
, col_393 decimal(1)
, col_394 decimal(1)
, col_395 decimal(1)
, col_396 decimal(1)
, col_397 decimal(1)
, col_398 decimal(1)
, col_399 decimal(1)
);
COMMENT ON TABLE pso_data_validator.dvt_many_cols IS 'Integration test table used to test validating many columns.';
INSERT INTO pso_data_validator.dvt_many_cols (id) values (1);

DROP TABLE pso_data_validator."dvt-identifier$_#";
CREATE TABLE pso_data_validator."dvt-identifier$_#"
(   id            int NOT NULL PRIMARY KEY
,   "col#hash"    varchar(10)
,   "col$dollar"  varchar(10)
,   "col-hyphen"  varchar(10)
,   "col@at"      varchar(10)
,   other_data    varchar(100)
);
COMMENT ON TABLE pso_data_validator."dvt-identifier$_#" IS 'Integration test table used to test non-standard characters in identifiers.';
INSERT INTO pso_data_validator."dvt-identifier$_#" VALUES (1,'#','$','-','@','Row 1');
INSERT INTO pso_data_validator."dvt-identifier$_#" VALUES (2,'#','$','-','@','Row 2');
INSERT INTO pso_data_validator."dvt-identifier$_#" VALUES (3,'#','$','-','@','Row 3');
INSERT INTO pso_data_validator."dvt-identifier$_#" VALUES (4,'#','$','-','@','Row 4');
INSERT INTO pso_data_validator."dvt-identifier$_#" VALUES (5,'#','$','-','@','Row 5');

DROP TABLE pso_data_validator.dvt_bool;
CREATE TABLE pso_data_validator.dvt_bool
(   id           int NOT NULL PRIMARY KEY
,   col_bool_dec boolean
,   col_bool_int boolean
,   col_bool_ch1 boolean
,   col_bool_chy boolean);
COMMENT ON TABLE pso_data_validator.dvt_bool IS 'Integration test table used to test boolean data type, especially in non-boolean columns.';
INSERT INTO pso_data_validator.dvt_bool VALUES (1,true,true,true,true);
INSERT INTO pso_data_validator.dvt_bool VALUES (2,false,false,false,false);

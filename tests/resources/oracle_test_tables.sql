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
,12345678901234567890,1234567890123456789012345,123.3,123456.3,12345678.3
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
,   col_tsltz       TIMESTAMP(6) WITH LOCAL TIME ZONE
,   col_interval_ds INTERVAL DAY(2) TO SECOND (3)
,   col_raw         RAW(16)
,   col_long_raw    LONG RAW
,   col_blob        BLOB
,   col_clob        CLOB
,   col_nclob       NCLOB
,   col_json        CLOB
,   col_jsonb       CLOB
);
ALTER TABLE pso_data_validator.dvt_ora2pg_types
ADD CONSTRAINT dvt_ora2pg_types_chk1 CHECK (col_json IS JSON) ENABLE;
ALTER TABLE pso_data_validator.dvt_ora2pg_types
ADD CONSTRAINT dvt_ora2pg_types_chk2 CHECK (col_jsonb IS JSON) ENABLE;
COMMENT ON TABLE pso_data_validator.dvt_ora2pg_types IS 'Oracle to PostgreSQL integration test table';

-- Literals below match corresponding table in postgresql_test_tables.sql
INSERT INTO pso_data_validator.dvt_ora2pg_types VALUES
(1,1111,123456789,123456789012345678,1234567890123456789012345
,0.1,123.1
--,123400,0.001
,123.123,123456.1,12345678.1
,'Hello DVT','A ','Hello DVT','A '
,DATE'1970-01-01',TIMESTAMP'1970-01-01 00:00:01.123456'
,to_timestamp_tz('1970-01-01 00:00:01.123456 00:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')
,to_timestamp_tz('1970-01-01 00:00:01.123456 00:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')
,INTERVAL '1 2:03:44.0' DAY TO SECOND(3)
,UTL_RAW.CAST_TO_RAW('DVT'),UTL_RAW.CAST_TO_RAW('DVT')
,UTL_RAW.CAST_TO_RAW('DVT'),'DVT A','DVT A'
,'{"dvt": 123, "status": "abc"}','{"dvt": 123, "status": "abc"}'
);
INSERT INTO pso_data_validator.dvt_ora2pg_types VALUES
(2,2222,123456789,123456789012345678,1234567890123456789012345
,123.12,123.11
--,123400,0.002
,123.123,123456.1,12345678.1
,'Hello DVT','B ','Hello DVT','B '
,DATE'1970-01-02',TIMESTAMP'1970-01-02 00:00:01.123456'
,to_timestamp_tz('1970-01-02 00:00:02.123456 -02:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')
,to_timestamp_tz('1970-01-02 00:00:02.123456 -02:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')
,INTERVAL '2 3:04:55.666' DAY TO SECOND(3)
,UTL_RAW.CAST_TO_RAW('DVT'),UTL_RAW.CAST_TO_RAW('DVT DVT')
,UTL_RAW.CAST_TO_RAW('DVT DVT'),'DVT B','DVT B'
,'{"dvt": 234, "status": "def"}','{"dvt": 234, "status": "def"}'
);
INSERT INTO pso_data_validator.dvt_ora2pg_types VALUES
(3,3333,123456789,123456789012345678,1234567890123456789012345
,123.123,123.11
--,123400,0.003
,123.123,123456.1,12345678.1
,'Hello DVT','C ','Hello DVT','C '
,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:01.123456'
,to_timestamp_tz('1970-01-03 00:00:03.123456 -03:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')
,to_timestamp_tz('1970-01-03 00:00:03.123456 -03:00','YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')
,INTERVAL '3 4:05:06.7' DAY TO SECOND(3)
,UTL_RAW.CAST_TO_RAW('DVT'),UTL_RAW.CAST_TO_RAW('DVT DVT DVT')
,UTL_RAW.CAST_TO_RAW('DVT DVT DVT'),'DVT C','DVT C'
,'{"dvt": 345, "status": "ghi"}','{"dvt": 345, "status": "ghi"}'
);
COMMIT;

DROP TABLE pso_data_validator.dvt_large_decimals;
CREATE TABLE pso_data_validator.dvt_large_decimals
(   id              NUMBER(38) NOT NULL PRIMARY KEY
,   col_data        VARCHAR2(10)
,   col_dec_18      NUMBER(18)
,   col_dec_38      NUMBER(38)
,   col_dec_38_9    NUMBER(38,9)
,   col_dec_38_30   NUMBER(38,30)
);
COMMENT ON TABLE pso_data_validator.dvt_large_decimals IS 'Large decimals integration test table';

INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(123456789012345678901234567890
,'Row 1'
,123456789012345678
,12345678901234567890123456789012345678
,12345678901234567890123456789.123456789
,12345678.123456789012345678901234567890);
INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(223456789012345678901234567890
,'Row 2'
,223456789012345678
,22345678901234567890123456789012345678
,22345678901234567890123456789.123456789
,22345678.123456789012345678901234567890);
INSERT INTO pso_data_validator.dvt_large_decimals VALUES
(323456789012345678901234567890
,'Row 3'
,323456789012345678
,32345678901234567890123456789012345678
,32345678901234567890123456789.123456789
,32345678.123456789012345678901234567890);
COMMIT;

DROP TABLE pso_data_validator.dvt_binary;
CREATE TABLE pso_data_validator.dvt_binary
(   binary_id       RAW(16) NOT NULL PRIMARY KEY
,   int_id          NUMBER(10) NOT NULL
,   other_data      VARCHAR2(100)
);
CREATE UNIQUE INDEX pso_data_validator.dvt_binary_int_id_uk ON pso_data_validator.dvt_binary (int_id);
COMMENT ON TABLE pso_data_validator.dvt_binary IS 'Integration test table used to test both binary pk matching and binary hash/concat comparisons.';
INSERT INTO pso_data_validator.dvt_binary VALUES (UTL_RAW.CAST_TO_RAW('DVT-key-1'), 1, 'Row 1');
INSERT INTO pso_data_validator.dvt_binary VALUES (UTL_RAW.CAST_TO_RAW('DVT-key-2'), 2, 'Row 2');
INSERT INTO pso_data_validator.dvt_binary VALUES (UTL_RAW.CAST_TO_RAW('DVT-key-3'), 3, 'Row 3');
INSERT INTO pso_data_validator.dvt_binary VALUES (UTL_RAW.CAST_TO_RAW('DVT-key-4'), 4, 'Row 4');
INSERT INTO pso_data_validator.dvt_binary VALUES (UTL_RAW.CAST_TO_RAW('DVT-key-5'), 5, 'Row 5');
COMMIT;

DROP TABLE pso_data_validator.dvt_string_id;
CREATE TABLE pso_data_validator.dvt_string_id
(   id          VARCHAR2(15) NOT NULL PRIMARY KEY
,   other_data  VARCHAR2(100)
);
COMMENT ON TABLE pso_data_validator.dvt_string_id IS 'Integration test table used to test string pk matching.';
INSERT INTO pso_data_validator.dvt_string_id VALUES ('DVT-key-1', 'Row 1');
INSERT INTO pso_data_validator.dvt_string_id VALUES ('DVT-key-2', 'Row 2');
INSERT INTO pso_data_validator.dvt_string_id VALUES ('DVT-key-3', 'Row 3');
INSERT INTO pso_data_validator.dvt_string_id VALUES ('DVT-key-4', 'Row 4');
INSERT INTO pso_data_validator.dvt_string_id VALUES ('DVT-key-5', 'Row 5');
COMMIT;

DROP TABLE pso_data_validator.dvt_char_id;
CREATE TABLE pso_data_validator.dvt_char_id
(   id          CHAR(6) NOT NULL PRIMARY KEY
,   other_data  VARCHAR2(100)
);
COMMENT ON TABLE pso_data_validator.dvt_char_id IS 'Integration test table used to test CHAR pk matching.';
INSERT INTO pso_data_validator.dvt_char_id VALUES ('DVT1', 'Row 1');
INSERT INTO pso_data_validator.dvt_char_id VALUES ('DVT2', 'Row 2');
INSERT INTO pso_data_validator.dvt_char_id VALUES ('DVT3', 'Row 3');
INSERT INTO pso_data_validator.dvt_char_id VALUES ('DVT4', 'Row 4');
INSERT INTO pso_data_validator.dvt_char_id VALUES ('DVT5', 'Row 5');
COMMIT;

DROP TABLE pso_data_validator.dvt_pangrams;
CREATE TABLE pso_data_validator.dvt_pangrams
(   id          NUMBER(5)
,   lang        VARCHAR2(100)
,   words       VARCHAR2(1000 CHAR)
,   words_en    VARCHAR2(1000)
,   CONSTRAINT dvt_pangrams_pk PRIMARY KEY (id)
);
COMMENT ON TABLE pso_data_validator.dvt_pangrams IS 'Integration test table used to test unicode characters.';
-- Text taken from Wikipedia, we cannot guarantee translations :-)
-- Be sure to set "export NLS_LANG=.AL32UTF8" if inserting via SQL*Plus.
INSERT INTO pso_data_validator.dvt_pangrams
VALUES (1,'Hebrew',
        'שפן אכל קצת גזר בטעם חסה, ודי',
        'A bunny ate some lettuce-flavored carrots, and he had enough');
INSERT INTO pso_data_validator.dvt_pangrams
VALUES (2,'Polish',
        'Pchnąć w tę łódź jeża lub ośm skrzyń fig',
        'Push a hedgehog or eight crates of figs in this boat');
INSERT INTO pso_data_validator.dvt_pangrams
VALUES (3,'Russian',
        'Съешь ещё этих мягких французских булок, да выпей же чаю',
        'Eat more of these soft French loaves and drink a tea');
INSERT INTO pso_data_validator.dvt_pangrams
VALUES (4,'Swedish',
        'Schweiz för lyxfjäder på qvist bakom ugn',
        'Switzerland brings luxury feather on branch behind oven');
INSERT INTO pso_data_validator.dvt_pangrams
VALUES (5,'Turkish',
        'Pijamalı hasta yağız şoföre çabucak güvendi',
        'The sick person in pyjamas quickly trusted the swarthy driver');
COMMIT;

DROP TABLE pso_data_validator.dvt_many_cols;
CREATE TABLE pso_data_validator.dvt_many_cols
( id NUMBER(5)
, col_001 VARCHAR2(2)
, col_002 VARCHAR2(2)
, col_003 VARCHAR2(2)
, col_004 VARCHAR2(2)
, col_005 VARCHAR2(2)
, col_006 VARCHAR2(2)
, col_007 VARCHAR2(2)
, col_008 VARCHAR2(2)
, col_009 VARCHAR2(2)
, col_010 VARCHAR2(2)
, col_011 NUMBER(1)
, col_012 NUMBER(1)
, col_013 NUMBER(1)
, col_014 NUMBER(1)
, col_015 NUMBER(1)
, col_016 NUMBER(1)
, col_017 NUMBER(1)
, col_018 NUMBER(1)
, col_019 NUMBER(1)
, col_020 NUMBER(1)
, col_021 VARCHAR2(2)
, col_022 VARCHAR2(2)
, col_023 VARCHAR2(2)
, col_024 VARCHAR2(2)
, col_025 VARCHAR2(2)
, col_026 VARCHAR2(2)
, col_027 VARCHAR2(2)
, col_028 VARCHAR2(2)
, col_029 VARCHAR2(2)
, col_030 VARCHAR2(2)
, col_031 NUMBER(1)
, col_032 NUMBER(1)
, col_033 NUMBER(1)
, col_034 NUMBER(1)
, col_035 NUMBER(1)
, col_036 NUMBER(1)
, col_037 NUMBER(1)
, col_038 NUMBER(1)
, col_039 NUMBER(1)
, col_040 NUMBER(1)
, col_041 VARCHAR2(2)
, col_042 VARCHAR2(2)
, col_043 VARCHAR2(2)
, col_044 VARCHAR2(2)
, col_045 VARCHAR2(2)
, col_046 VARCHAR2(2)
, col_047 VARCHAR2(2)
, col_048 VARCHAR2(2)
, col_049 VARCHAR2(2)
, col_050 VARCHAR2(2)
, col_051 NUMBER(1)
, col_052 NUMBER(1)
, col_053 NUMBER(1)
, col_054 NUMBER(1)
, col_055 NUMBER(1)
, col_056 NUMBER(1)
, col_057 NUMBER(1)
, col_058 NUMBER(1)
, col_059 NUMBER(1)
, col_060 NUMBER(1)
, col_061 VARCHAR2(2)
, col_062 VARCHAR2(2)
, col_063 VARCHAR2(2)
, col_064 VARCHAR2(2)
, col_065 VARCHAR2(2)
, col_066 VARCHAR2(2)
, col_067 VARCHAR2(2)
, col_068 VARCHAR2(2)
, col_069 VARCHAR2(2)
, col_070 VARCHAR2(2)
, col_071 NUMBER(1)
, col_072 NUMBER(1)
, col_073 NUMBER(1)
, col_074 NUMBER(1)
, col_075 NUMBER(1)
, col_076 NUMBER(1)
, col_077 NUMBER(1)
, col_078 NUMBER(1)
, col_079 NUMBER(1)
, col_080 NUMBER(1)
, col_081 VARCHAR2(2)
, col_082 VARCHAR2(2)
, col_083 VARCHAR2(2)
, col_084 VARCHAR2(2)
, col_085 VARCHAR2(2)
, col_086 VARCHAR2(2)
, col_087 VARCHAR2(2)
, col_088 VARCHAR2(2)
, col_089 VARCHAR2(2)
, col_090 VARCHAR2(2)
, col_091 NUMBER(1)
, col_092 NUMBER(1)
, col_093 NUMBER(1)
, col_094 NUMBER(1)
, col_095 NUMBER(1)
, col_096 NUMBER(1)
, col_097 NUMBER(1)
, col_098 NUMBER(1)
, col_099 NUMBER(1)
, col_100 NUMBER(1)
, col_101 VARCHAR2(2)
, col_102 VARCHAR2(2)
, col_103 VARCHAR2(2)
, col_104 VARCHAR2(2)
, col_105 VARCHAR2(2)
, col_106 VARCHAR2(2)
, col_107 VARCHAR2(2)
, col_108 VARCHAR2(2)
, col_109 VARCHAR2(2)
, col_110 VARCHAR2(2)
, col_111 NUMBER(1)
, col_112 NUMBER(1)
, col_113 NUMBER(1)
, col_114 NUMBER(1)
, col_115 NUMBER(1)
, col_116 NUMBER(1)
, col_117 NUMBER(1)
, col_118 NUMBER(1)
, col_119 NUMBER(1)
, col_120 NUMBER(1)
, col_121 VARCHAR2(2)
, col_122 VARCHAR2(2)
, col_123 VARCHAR2(2)
, col_124 VARCHAR2(2)
, col_125 VARCHAR2(2)
, col_126 VARCHAR2(2)
, col_127 VARCHAR2(2)
, col_128 VARCHAR2(2)
, col_129 VARCHAR2(2)
, col_130 VARCHAR2(2)
, col_131 NUMBER(1)
, col_132 NUMBER(1)
, col_133 NUMBER(1)
, col_134 NUMBER(1)
, col_135 NUMBER(1)
, col_136 NUMBER(1)
, col_137 NUMBER(1)
, col_138 NUMBER(1)
, col_139 NUMBER(1)
, col_140 NUMBER(1)
, col_141 VARCHAR2(2)
, col_142 VARCHAR2(2)
, col_143 VARCHAR2(2)
, col_144 VARCHAR2(2)
, col_145 VARCHAR2(2)
, col_146 VARCHAR2(2)
, col_147 VARCHAR2(2)
, col_148 VARCHAR2(2)
, col_149 VARCHAR2(2)
, col_150 VARCHAR2(2)
, col_151 NUMBER(1)
, col_152 NUMBER(1)
, col_153 NUMBER(1)
, col_154 NUMBER(1)
, col_155 NUMBER(1)
, col_156 NUMBER(1)
, col_157 NUMBER(1)
, col_158 NUMBER(1)
, col_159 NUMBER(1)
, col_160 NUMBER(1)
, col_161 VARCHAR2(2)
, col_162 VARCHAR2(2)
, col_163 VARCHAR2(2)
, col_164 VARCHAR2(2)
, col_165 VARCHAR2(2)
, col_166 VARCHAR2(2)
, col_167 VARCHAR2(2)
, col_168 VARCHAR2(2)
, col_169 VARCHAR2(2)
, col_170 VARCHAR2(2)
, col_171 NUMBER(1)
, col_172 NUMBER(1)
, col_173 NUMBER(1)
, col_174 NUMBER(1)
, col_175 NUMBER(1)
, col_176 NUMBER(1)
, col_177 NUMBER(1)
, col_178 NUMBER(1)
, col_179 NUMBER(1)
, col_180 NUMBER(1)
, col_181 VARCHAR2(2)
, col_182 VARCHAR2(2)
, col_183 VARCHAR2(2)
, col_184 VARCHAR2(2)
, col_185 VARCHAR2(2)
, col_186 VARCHAR2(2)
, col_187 VARCHAR2(2)
, col_188 VARCHAR2(2)
, col_189 VARCHAR2(2)
, col_190 VARCHAR2(2)
, col_191 NUMBER(1)
, col_192 NUMBER(1)
, col_193 NUMBER(1)
, col_194 NUMBER(1)
, col_195 NUMBER(1)
, col_196 NUMBER(1)
, col_197 NUMBER(1)
, col_198 NUMBER(1)
, col_199 NUMBER(1)
, col_200 NUMBER(1)
, col_201 VARCHAR2(2)
, col_202 VARCHAR2(2)
, col_203 VARCHAR2(2)
, col_204 VARCHAR2(2)
, col_205 VARCHAR2(2)
, col_206 VARCHAR2(2)
, col_207 VARCHAR2(2)
, col_208 VARCHAR2(2)
, col_209 VARCHAR2(2)
, col_210 VARCHAR2(2)
, col_211 NUMBER(1)
, col_212 NUMBER(1)
, col_213 NUMBER(1)
, col_214 NUMBER(1)
, col_215 NUMBER(1)
, col_216 NUMBER(1)
, col_217 NUMBER(1)
, col_218 NUMBER(1)
, col_219 NUMBER(1)
, col_220 NUMBER(1)
, col_221 VARCHAR2(2)
, col_222 VARCHAR2(2)
, col_223 VARCHAR2(2)
, col_224 VARCHAR2(2)
, col_225 VARCHAR2(2)
, col_226 VARCHAR2(2)
, col_227 VARCHAR2(2)
, col_228 VARCHAR2(2)
, col_229 VARCHAR2(2)
, col_230 VARCHAR2(2)
, col_231 NUMBER(1)
, col_232 NUMBER(1)
, col_233 NUMBER(1)
, col_234 NUMBER(1)
, col_235 NUMBER(1)
, col_236 NUMBER(1)
, col_237 NUMBER(1)
, col_238 NUMBER(1)
, col_239 NUMBER(1)
, col_240 NUMBER(1)
, col_241 VARCHAR2(2)
, col_242 VARCHAR2(2)
, col_243 VARCHAR2(2)
, col_244 VARCHAR2(2)
, col_245 VARCHAR2(2)
, col_246 VARCHAR2(2)
, col_247 VARCHAR2(2)
, col_248 VARCHAR2(2)
, col_249 VARCHAR2(2)
, col_250 VARCHAR2(2)
, col_251 NUMBER(1)
, col_252 NUMBER(1)
, col_253 NUMBER(1)
, col_254 NUMBER(1)
, col_255 NUMBER(1)
, col_256 NUMBER(1)
, col_257 NUMBER(1)
, col_258 NUMBER(1)
, col_259 NUMBER(1)
, col_260 NUMBER(1)
, col_261 VARCHAR2(2)
, col_262 VARCHAR2(2)
, col_263 VARCHAR2(2)
, col_264 VARCHAR2(2)
, col_265 VARCHAR2(2)
, col_266 VARCHAR2(2)
, col_267 VARCHAR2(2)
, col_268 VARCHAR2(2)
, col_269 VARCHAR2(2)
, col_270 VARCHAR2(2)
, col_271 NUMBER(1)
, col_272 NUMBER(1)
, col_273 NUMBER(1)
, col_274 NUMBER(1)
, col_275 NUMBER(1)
, col_276 NUMBER(1)
, col_277 NUMBER(1)
, col_278 NUMBER(1)
, col_279 NUMBER(1)
, col_280 NUMBER(1)
, col_281 VARCHAR2(2)
, col_282 VARCHAR2(2)
, col_283 VARCHAR2(2)
, col_284 VARCHAR2(2)
, col_285 VARCHAR2(2)
, col_286 VARCHAR2(2)
, col_287 VARCHAR2(2)
, col_288 VARCHAR2(2)
, col_289 VARCHAR2(2)
, col_290 VARCHAR2(2)
, col_291 NUMBER(1)
, col_292 NUMBER(1)
, col_293 NUMBER(1)
, col_294 NUMBER(1)
, col_295 NUMBER(1)
, col_296 NUMBER(1)
, col_297 NUMBER(1)
, col_298 NUMBER(1)
, col_299 NUMBER(1)
, col_300 NUMBER(1)
, col_301 VARCHAR2(2)
, col_302 VARCHAR2(2)
, col_303 VARCHAR2(2)
, col_304 VARCHAR2(2)
, col_305 VARCHAR2(2)
, col_306 VARCHAR2(2)
, col_307 VARCHAR2(2)
, col_308 VARCHAR2(2)
, col_309 VARCHAR2(2)
, col_310 VARCHAR2(2)
, col_311 NUMBER(1)
, col_312 NUMBER(1)
, col_313 NUMBER(1)
, col_314 NUMBER(1)
, col_315 NUMBER(1)
, col_316 NUMBER(1)
, col_317 NUMBER(1)
, col_318 NUMBER(1)
, col_319 NUMBER(1)
, col_320 NUMBER(1)
, col_321 VARCHAR2(2)
, col_322 VARCHAR2(2)
, col_323 VARCHAR2(2)
, col_324 VARCHAR2(2)
, col_325 VARCHAR2(2)
, col_326 VARCHAR2(2)
, col_327 VARCHAR2(2)
, col_328 VARCHAR2(2)
, col_329 VARCHAR2(2)
, col_330 VARCHAR2(2)
, col_331 NUMBER(1)
, col_332 NUMBER(1)
, col_333 NUMBER(1)
, col_334 NUMBER(1)
, col_335 NUMBER(1)
, col_336 NUMBER(1)
, col_337 NUMBER(1)
, col_338 NUMBER(1)
, col_339 NUMBER(1)
, col_340 NUMBER(1)
, col_341 VARCHAR2(2)
, col_342 VARCHAR2(2)
, col_343 VARCHAR2(2)
, col_344 VARCHAR2(2)
, col_345 VARCHAR2(2)
, col_346 VARCHAR2(2)
, col_347 VARCHAR2(2)
, col_348 VARCHAR2(2)
, col_349 VARCHAR2(2)
, col_350 VARCHAR2(2)
, col_351 NUMBER(1)
, col_352 NUMBER(1)
, col_353 NUMBER(1)
, col_354 NUMBER(1)
, col_355 NUMBER(1)
, col_356 NUMBER(1)
, col_357 NUMBER(1)
, col_358 NUMBER(1)
, col_359 NUMBER(1)
, col_360 NUMBER(1)
, col_361 VARCHAR2(2)
, col_362 VARCHAR2(2)
, col_363 VARCHAR2(2)
, col_364 VARCHAR2(2)
, col_365 VARCHAR2(2)
, col_366 VARCHAR2(2)
, col_367 VARCHAR2(2)
, col_368 VARCHAR2(2)
, col_369 VARCHAR2(2)
, col_370 VARCHAR2(2)
, col_371 NUMBER(1)
, col_372 NUMBER(1)
, col_373 NUMBER(1)
, col_374 NUMBER(1)
, col_375 NUMBER(1)
, col_376 NUMBER(1)
, col_377 NUMBER(1)
, col_378 NUMBER(1)
, col_379 NUMBER(1)
, col_380 NUMBER(1)
, col_381 VARCHAR2(2)
, col_382 VARCHAR2(2)
, col_383 VARCHAR2(2)
, col_384 VARCHAR2(2)
, col_385 VARCHAR2(2)
, col_386 VARCHAR2(2)
, col_387 VARCHAR2(2)
, col_388 VARCHAR2(2)
, col_389 VARCHAR2(2)
, col_390 VARCHAR2(2)
, col_391 NUMBER(1)
, col_392 NUMBER(1)
, col_393 NUMBER(1)
, col_394 NUMBER(1)
, col_395 NUMBER(1)
, col_396 NUMBER(1)
, col_397 NUMBER(1)
, col_398 NUMBER(1)
, col_399 NUMBER(1)
);
COMMENT ON TABLE pso_data_validator.dvt_many_cols IS 'Integration test table used to test validating many columns.';
INSERT INTO pso_data_validator.dvt_many_cols (id) VALUES (1);
COMMIT;

DROP TABLE pso_data_validator."DVT-IDENTIFIER$_#";
CREATE TABLE pso_data_validator."DVT-IDENTIFIER$_#"
(   id            NUMBER(6) NOT NULL PRIMARY KEY
,   "COL#HASH"    VARCHAR2(10)
,   "COL$DOLLAR"  VARCHAR2(10)
,   "COL-HYPHEN"  VARCHAR2(10)
,   "COL@AT"      VARCHAR2(10)
,   other_data    VARCHAR2(100)
);
COMMENT ON TABLE pso_data_validator."DVT-IDENTIFIER$_#" IS 'Integration test table used to test non-standard characters in identifiers.';
INSERT INTO pso_data_validator."DVT-IDENTIFIER$_#" VALUES (1,'#','$','-','@','Row 1');
INSERT INTO pso_data_validator."DVT-IDENTIFIER$_#" VALUES (2,'#','$','-','@','Row 2');
INSERT INTO pso_data_validator."DVT-IDENTIFIER$_#" VALUES (3,'#','$','-','@','Row 3');
INSERT INTO pso_data_validator."DVT-IDENTIFIER$_#" VALUES (4,'#','$','-','@','Row 4');
INSERT INTO pso_data_validator."DVT-IDENTIFIER$_#" VALUES (5,'#','$','-','@','Row 5');
COMMIT;

DROP TABLE pso_data_validator.dvt_bool;
CREATE TABLE pso_data_validator.dvt_bool
(   id           NUMBER(5) NOT NULL PRIMARY KEY
,   col_bool_dec NUMBER(1)
,   col_bool_int NUMBER(1)
,   col_bool_ch1 CHAR(1)
,   col_bool_chy CHAR(1));
COMMENT ON TABLE pso_data_validator.dvt_bool IS 'Integration test table used to test boolean data type, especially in non-boolean columns.';
INSERT INTO pso_data_validator.dvt_bool VALUES (1,1,1,'1','Y');
INSERT INTO pso_data_validator.dvt_bool VALUES (2,0,0,'0','N');
COMMIT;

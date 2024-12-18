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

CREATE OR REPLACE TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES (
	ID INT NOT NULL PRIMARY KEY,
	COL_INT8 TINYINT,
	COL_INT16 SMALLINT,
	COL_INT32 INT,
	COL_INT64 BIGINT,
	COL_DEC_20 NUMBER(20),
	COL_DEC_38 NUMBER(38),
	COL_DEC_10_2 NUMBER(10,2),
	COL_FLOAT32 FLOAT,
	COL_FLOAT64 FLOAT,
	COL_VARCHAR_30 VARCHAR(30),
	COL_CHAR_2 CHAR(2),
	COL_STRING STRING,
	COL_DATE DATE,
	COL_DATETIME DATETIME,
	COL_TSTZ TIMESTAMP_TZ
);

INSERT INTO PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES VALUES
(1,1,1,1,1
 ,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1
 ,'Hello DVT','A ','Hello DVT'
 ,DATE'1970-01-01',TIMESTAMP'1970-01-01 00:00:01'
 ,'1970-01-01 00:00:01 -01:00'),
(2,2,2,2,2
 ,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2
 ,'Hello DVT','B ','Hello DVT'
 ,DATE'1970-01-02',TIMESTAMP'1970-01-02 00:00:02'
 ,'1970-01-02 00:00:02 -02:00'),
(3,3,3,3,3
 ,12345678901234567890,1234567890123456789012345,123.3,123456.3,12345678.3
 ,'Hello DVT','C ','Hello DVT'
 ,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:03'
 ,'1970-01-03 00:00:03 -03:00');

CREATE OR REPLACE VIEW PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES_VW AS
SELECT * FROM PSO_DATA_VALIDATOR.PUBLIC.DVT_CORE_TYPES;


CREATE OR REPLACE TABLE PSO_DATA_VALIDATOR.PUBLIC.TEST_GENERATE_PARTITIONS (
        COURSE_ID VARCHAR(6),
        QUARTER_ID INTEGER,
        STUDENT_ID INTEGER,
        GRADE NUMERIC,
        PRIMARY KEY (COURSE_ID, QUARTER_ID, STUDENT_ID));

INSERT INTO PSO_DATA_VALIDATOR.PUBLIC.TEST_GENERATE_PARTITIONS (COURSE_ID, QUARTER_ID, STUDENT_ID, GRADE) VALUES
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

DROP TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_NULL_NOT_NULL;
CREATE TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_NULL_NOT_NULL
(   col_nn             TIMESTAMP(0) NOT NULL
,   col_nullable       TIMESTAMP(0)
,   col_src_nn_trg_n   TIMESTAMP(0) NOT NULL
,   col_src_n_trg_nn   TIMESTAMP(0)
);
COMMENT ON TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_NULL_NOT_NULL IS 'Nullable integration test table, Oracle is assumed to be a DVT source (not target).';

DROP TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_BINARY;
CREATE TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_BINARY
(   BINARY_ID       BINARY(16) NOT NULL PRIMARY KEY
,   INT_ID          NUMBER(10) NOT NULL
,   OTHER_DATA      VARCHAR2(100)
);
COMMENT ON TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_BINARY IS 'Integration test table used to test both binary pk matching and binary hash/concat comparisons.';
INSERT INTO PSO_DATA_VALIDATOR.PUBLIC.DVT_BINARY VALUES
(TO_BINARY('DVT-key-1', 'UTF-8'), 1, 'Row 1'),
(TO_BINARY('DVT-key-2', 'UTF-8'), 2, 'Row 2'),
(TO_BINARY('DVT-key-3', 'UTF-8'), 3, 'Row 3'),
(TO_BINARY('DVT-key-4', 'UTF-8'), 4, 'Row 4'),
(TO_BINARY('DVT-key-5', 'UTF-8'), 5, 'Row 5');

DROP TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_CHAR_ID;
-- Snowflake deviates from common CHAR semantics, strings are not space-padded.
CREATE TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_CHAR_ID
(   id          CHAR(6) NOT NULL PRIMARY KEY
,   other_data  VARCHAR(100)
);
COMMENT ON TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_CHAR_ID IS 'Integration test table used to test CHAR pk matching.';
INSERT INTO PSO_DATA_VALIDATOR.PUBLIC.DVT_CHAR_ID VALUES
('DVT1  ', 'Row 1'),
('DVT2  ', 'Row 2'),
('DVT3  ', 'Row 3'),
('DVT4  ', 'Row 4'),
('DVT5  ', 'Row 5');

DROP TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_PANGRAMS;
CREATE TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_PANGRAMS
(   id          NUMBER(5)
,   lang        VARCHAR(100)
,   words       VARCHAR(1000)
,   words_en    VARCHAR(1000)
,   CONSTRAINT dvt_pangrams_pk PRIMARY KEY (id)
);
COMMENT ON TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_PANGRAMS IS 'Integration test table used to test unicode characters.';
-- Text taken from Wikipedia, we cannot guarantee translations :-)
INSERT INTO PSO_DATA_VALIDATOR.PUBLIC.DVT_PANGRAMS VALUES
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


DROP TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_MANY_COLS;
CREATE TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_MANY_COLS
( id NUMBER(5)
, col_001 VARCHAR(2)
, col_002 VARCHAR(2)
, col_003 VARCHAR(2)
, col_004 VARCHAR(2)
, col_005 VARCHAR(2)
, col_006 VARCHAR(2)
, col_007 VARCHAR(2)
, col_008 VARCHAR(2)
, col_009 VARCHAR(2)
, col_010 VARCHAR(2)
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
, col_021 VARCHAR(2)
, col_022 VARCHAR(2)
, col_023 VARCHAR(2)
, col_024 VARCHAR(2)
, col_025 VARCHAR(2)
, col_026 VARCHAR(2)
, col_027 VARCHAR(2)
, col_028 VARCHAR(2)
, col_029 VARCHAR(2)
, col_030 VARCHAR(2)
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
, col_041 VARCHAR(2)
, col_042 VARCHAR(2)
, col_043 VARCHAR(2)
, col_044 VARCHAR(2)
, col_045 VARCHAR(2)
, col_046 VARCHAR(2)
, col_047 VARCHAR(2)
, col_048 VARCHAR(2)
, col_049 VARCHAR(2)
, col_050 VARCHAR(2)
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
, col_061 VARCHAR(2)
, col_062 VARCHAR(2)
, col_063 VARCHAR(2)
, col_064 VARCHAR(2)
, col_065 VARCHAR(2)
, col_066 VARCHAR(2)
, col_067 VARCHAR(2)
, col_068 VARCHAR(2)
, col_069 VARCHAR(2)
, col_070 VARCHAR(2)
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
, col_081 VARCHAR(2)
, col_082 VARCHAR(2)
, col_083 VARCHAR(2)
, col_084 VARCHAR(2)
, col_085 VARCHAR(2)
, col_086 VARCHAR(2)
, col_087 VARCHAR(2)
, col_088 VARCHAR(2)
, col_089 VARCHAR(2)
, col_090 VARCHAR(2)
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
, col_101 VARCHAR(2)
, col_102 VARCHAR(2)
, col_103 VARCHAR(2)
, col_104 VARCHAR(2)
, col_105 VARCHAR(2)
, col_106 VARCHAR(2)
, col_107 VARCHAR(2)
, col_108 VARCHAR(2)
, col_109 VARCHAR(2)
, col_110 VARCHAR(2)
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
, col_121 VARCHAR(2)
, col_122 VARCHAR(2)
, col_123 VARCHAR(2)
, col_124 VARCHAR(2)
, col_125 VARCHAR(2)
, col_126 VARCHAR(2)
, col_127 VARCHAR(2)
, col_128 VARCHAR(2)
, col_129 VARCHAR(2)
, col_130 VARCHAR(2)
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
, col_141 VARCHAR(2)
, col_142 VARCHAR(2)
, col_143 VARCHAR(2)
, col_144 VARCHAR(2)
, col_145 VARCHAR(2)
, col_146 VARCHAR(2)
, col_147 VARCHAR(2)
, col_148 VARCHAR(2)
, col_149 VARCHAR(2)
, col_150 VARCHAR(2)
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
, col_161 VARCHAR(2)
, col_162 VARCHAR(2)
, col_163 VARCHAR(2)
, col_164 VARCHAR(2)
, col_165 VARCHAR(2)
, col_166 VARCHAR(2)
, col_167 VARCHAR(2)
, col_168 VARCHAR(2)
, col_169 VARCHAR(2)
, col_170 VARCHAR(2)
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
, col_181 VARCHAR(2)
, col_182 VARCHAR(2)
, col_183 VARCHAR(2)
, col_184 VARCHAR(2)
, col_185 VARCHAR(2)
, col_186 VARCHAR(2)
, col_187 VARCHAR(2)
, col_188 VARCHAR(2)
, col_189 VARCHAR(2)
, col_190 VARCHAR(2)
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
, col_201 VARCHAR(2)
, col_202 VARCHAR(2)
, col_203 VARCHAR(2)
, col_204 VARCHAR(2)
, col_205 VARCHAR(2)
, col_206 VARCHAR(2)
, col_207 VARCHAR(2)
, col_208 VARCHAR(2)
, col_209 VARCHAR(2)
, col_210 VARCHAR(2)
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
, col_221 VARCHAR(2)
, col_222 VARCHAR(2)
, col_223 VARCHAR(2)
, col_224 VARCHAR(2)
, col_225 VARCHAR(2)
, col_226 VARCHAR(2)
, col_227 VARCHAR(2)
, col_228 VARCHAR(2)
, col_229 VARCHAR(2)
, col_230 VARCHAR(2)
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
, col_241 VARCHAR(2)
, col_242 VARCHAR(2)
, col_243 VARCHAR(2)
, col_244 VARCHAR(2)
, col_245 VARCHAR(2)
, col_246 VARCHAR(2)
, col_247 VARCHAR(2)
, col_248 VARCHAR(2)
, col_249 VARCHAR(2)
, col_250 VARCHAR(2)
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
, col_261 VARCHAR(2)
, col_262 VARCHAR(2)
, col_263 VARCHAR(2)
, col_264 VARCHAR(2)
, col_265 VARCHAR(2)
, col_266 VARCHAR(2)
, col_267 VARCHAR(2)
, col_268 VARCHAR(2)
, col_269 VARCHAR(2)
, col_270 VARCHAR(2)
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
, col_281 VARCHAR(2)
, col_282 VARCHAR(2)
, col_283 VARCHAR(2)
, col_284 VARCHAR(2)
, col_285 VARCHAR(2)
, col_286 VARCHAR(2)
, col_287 VARCHAR(2)
, col_288 VARCHAR(2)
, col_289 VARCHAR(2)
, col_290 VARCHAR(2)
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
, col_301 VARCHAR(2)
, col_302 VARCHAR(2)
, col_303 VARCHAR(2)
, col_304 VARCHAR(2)
, col_305 VARCHAR(2)
, col_306 VARCHAR(2)
, col_307 VARCHAR(2)
, col_308 VARCHAR(2)
, col_309 VARCHAR(2)
, col_310 VARCHAR(2)
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
, col_321 VARCHAR(2)
, col_322 VARCHAR(2)
, col_323 VARCHAR(2)
, col_324 VARCHAR(2)
, col_325 VARCHAR(2)
, col_326 VARCHAR(2)
, col_327 VARCHAR(2)
, col_328 VARCHAR(2)
, col_329 VARCHAR(2)
, col_330 VARCHAR(2)
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
, col_341 VARCHAR(2)
, col_342 VARCHAR(2)
, col_343 VARCHAR(2)
, col_344 VARCHAR(2)
, col_345 VARCHAR(2)
, col_346 VARCHAR(2)
, col_347 VARCHAR(2)
, col_348 VARCHAR(2)
, col_349 VARCHAR(2)
, col_350 VARCHAR(2)
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
, col_361 VARCHAR(2)
, col_362 VARCHAR(2)
, col_363 VARCHAR(2)
, col_364 VARCHAR(2)
, col_365 VARCHAR(2)
, col_366 VARCHAR(2)
, col_367 VARCHAR(2)
, col_368 VARCHAR(2)
, col_369 VARCHAR(2)
, col_370 VARCHAR(2)
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
, col_381 VARCHAR(2)
, col_382 VARCHAR(2)
, col_383 VARCHAR(2)
, col_384 VARCHAR(2)
, col_385 VARCHAR(2)
, col_386 VARCHAR(2)
, col_387 VARCHAR(2)
, col_388 VARCHAR(2)
, col_389 VARCHAR(2)
, col_390 VARCHAR(2)
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
COMMENT ON TABLE PSO_DATA_VALIDATOR.PUBLIC.DVT_MANY_COLS IS 'Integration test table used to test validating many columns.';
INSERT INTO PSO_DATA_VALIDATOR.PUBLIC.DVT_MANY_COLS (id) VALUES (1);

DROP TABLE PSO_DATA_VALIDATOR.PUBLIC."DVT-IDENTIFIER$_#";
CREATE TABLE PSO_DATA_VALIDATOR.PUBLIC."DVT-IDENTIFIER$_#"
(   id            NUMBER(6) NOT NULL PRIMARY KEY
,   "COL#HASH"    VARCHAR(10)
,   "COL$DOLLAR"  VARCHAR(10)
,   "COL-HYPHEN"  VARCHAR(10)
,   "COL@AT"      VARCHAR(10)
,   other_data    VARCHAR(100)
);
COMMENT ON TABLE PSO_DATA_VALIDATOR.PUBLIC."DVT-IDENTIFIER$_#" IS 'Integration test table used to test non-standard characters in identifiers.';
INSERT INTO PSO_DATA_VALIDATOR.PUBLIC."DVT-IDENTIFIER$_#"
VALUES
(1,'#','$','-','@','Row 1'),
(2,'#','$','-','@','Row 2'),
(3,'#','$','-','@','Row 3'),
(4,'#','$','-','@','Row 4'),
(5,'#','$','-','@','Row 5');

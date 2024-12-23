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

CREATE DATABASE pso_data_validator;

DROP TABLE IF EXISTS `pso_data_validator`.`entries`;
CREATE TABLE `pso_data_validator`.`entries` (
  `guestName` varchar(255) DEFAULT NULL,
  `content` varchar(255) DEFAULT NULL,
  `entryID` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`entryID`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8;

INSERT INTO `pso_data_validator`.`entries` VALUES
('first guest','I got here!',1),('second guest','Me too!',2),
('This Guy','More data Coming',3),('Joe','Me too!',4),
('John','I got here!',5),('Alex','Me too!',6),('Zoe','zippp!',7);
COMMIT;

DROP TABLE IF EXISTS `pso_data_validator`.`test_data_types_mysql_row`;
CREATE TABLE `pso_data_validator`.`test_data_types_mysql_row` (
  `serial_col` int(11) NOT NULL AUTO_INCREMENT,
  `int_col` int(3) DEFAULT NULL,
  `text_col` char(100) DEFAULT NULL,
  `char_col` char(30) DEFAULT NULL,
  `varchar_col` varchar(255) DEFAULT NULL,
  `float_col` float(5,2) DEFAULT NULL,
  `decimal_col` decimal(5,2) DEFAULT NULL,
  `datetime_col` datetime DEFAULT NULL,
  `date_col` date DEFAULT NULL,
  PRIMARY KEY (`serial_col`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8;

INSERT INTO `pso_data_validator`.`test_data_types_mysql_row` VALUES
(1,10,'row1_text','1','row1_varchar',10.10,10.10,'2016-06-22 19:10:25','2020-01-01'),
(2,20,'row2_text','2','row2_varchar',20.20,20.20,'2016-06-22 19:20:25','2020-02-02'),
(3,30,'row3_text','3','row3_varchar',30.20,30.20,'2016-06-22 19:30:25','2020-03-03'),
(4,40,'row4_text','4','row4_varchar',40.20,40.40,'2016-06-22 19:30:45','2020-04-04'),
(5,50,'row5_text','5','row5_varchar',50.20,50.40,'2016-06-22 19:50:44','2020-05-05'),
(6,60,'row6_text','6','row6_varchar',60.20,60.40,'2016-01-22 19:51:44','2020-06-06'),
(7,70,'row7_text','7','row7_varchar',70.20,70.40,'2017-01-22 19:51:44','2020-06-07'),
(8,80,'row8_text','8','row8_varchar',80.20,80.40,'2018-01-22 19:51:44','2020-06-08'),
(9,90,'row9_text','9','row9_varchar',90.20,90.40,'2019-01-22 19:51:44','2020-06-09'),
(10,100,'row10_text','1','row10_varchar',100.20,100.40,'2020-01-22 19:51:44','2021-06-09');
COMMIT;

DROP TABLE IF EXISTS `pso_data_validator`.`dvt_core_types`;
CREATE TABLE `pso_data_validator`.`dvt_core_types`
(   id              int NOT NULL PRIMARY KEY
,   col_int8        tinyint
,   col_int16       smallint
,   col_int32       int
,   col_int64       bigint
,   col_dec_20      decimal(20)
,   col_dec_38      decimal(38)
,   col_dec_10_2    decimal(10,2)
,   col_float32     float
,   col_float64     double
,   col_varchar_30  varchar(30)
,   col_char_2      char(2)
,   col_string      varchar(21000)
,   col_date        date
,   col_datetime    datetime(3)
,   col_tstz        timestamp(3)
) COMMENT='Core data types integration test table';

SET time_zone = '+00:00';
INSERT INTO `pso_data_validator`.`dvt_core_types` VALUES
(1,1,1,1,1
 ,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1
 ,'Hello DVT','A ','Hello DVT'
 ,'1970-01-01','1970-01-01 00:00:01','1970-01-01 01:00:01'),
(2,2,2,2,2
 ,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2
 ,'Hello DVT','B ','Hello DVT'
 ,'1970-01-02','1970-01-02 00:00:02','1970-01-02 02:00:02'),
(3,3,3,3,3
 ,12345678901234567890,1234567890123456789012345,123.3,123456.3,12345678.3
 ,'Hello DVT','C ','Hello DVT'
 ,'1970-01-03','1970-01-03 00:00:03','1970-01-03 03:00:03');

CREATE VIEW `pso_data_validator`.`dvt_core_types_vw` AS
SELECT * FROM `pso_data_validator`.`dvt_core_types`;


DROP TABLE IF EXISTS `pso_data_validator`.`dvt_null_not_null`;
CREATE TABLE `pso_data_validator`.`dvt_null_not_null`
(   col_nn             datetime(0) NOT NULL
,   col_nullable       datetime(0)
,   col_src_nn_trg_n   datetime(0) NOT NULL
,   col_src_n_trg_nn   datetime(0)
) COMMENT 'Nullable integration test table, MySQL is assumed to be a DVT source (not target).';

DROP TABLE IF EXISTS `pso_data_validator`.`dvt_binary`;
CREATE TABLE `pso_data_validator`.`dvt_binary`
(   binary_id       varbinary(16) NOT NULL PRIMARY KEY
,   int_id          int NOT NULL
,   other_data      varchar(100)
) COMMENT 'Integration test table used to test both binary pk matching and binary hash/concat comparisons.';
CREATE UNIQUE INDEX `dvt_binary_int_id_uk` ON `pso_data_validator`.`dvt_binary` (int_id);
INSERT INTO pso_data_validator.dvt_binary VALUES
('DVT-key-1', 1, 'Row 1'),
('DVT-key-2', 2, 'Row 2'),
('DVT-key-3', 3, 'Row 3'),
('DVT-key-4', 4, 'Row 4'),
('DVT-key-5', 5, 'Row 5');

DROP TABLE IF EXISTS `pso_data_validator`.`dvt_char_id`;
CREATE TABLE `pso_data_validator`.`dvt_char_id`
(   id          char(6) NOT NULL PRIMARY KEY
,   other_data  varchar(100)
) COMMENT 'Integration test table used to test CHAR pk matching.';
INSERT INTO `pso_data_validator`.`dvt_char_id` VALUES
('DVT1', 'Row 1'),
('DVT2', 'Row 2'),
('DVT3', 'Row 3'),
('DVT4', 'Row 4'),
('DVT5', 'Row 5');

DROP TABLE IF EXISTS `pso_data_validator`.`dvt_pangrams`;
CREATE TABLE `pso_data_validator`.`dvt_pangrams`
(   id          int NOT NULL PRIMARY KEY
,   lang        varchar(100)
,   words       varchar(1000) CHARACTER SET utf8 COLLATE utf8_unicode_ci
,   words_en    varchar(1000)
) COMMENT 'Integration test table used to test unicode characters.';
-- Text taken from Wikipedia, we cannot guarantee translations :-)
INSERT INTO `pso_data_validator`.`dvt_pangrams` VALUES
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


DROP TABLE IF EXISTS `pso_data_validator`.`dvt_many_cols`;
CREATE TABLE `pso_data_validator`.`dvt_many_cols`
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
) COMMENT 'Integration test table used to test validating many columns.';
INSERT INTO `pso_data_validator`.`dvt_many_cols` (id) values (1);

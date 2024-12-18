-- Copyright 2021 Google LLC
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

CREATE TABLE students_pointer
(
  id INT64,
  name STRING(30),
  division INT64,
  marks INT64,
  exam STRING(30),
  overall_pointer FLOAT64,
  date_of_exam TIMESTAMP
)
PRIMARY KEY (id);

CREATE TABLE functional_alltypes
(
  id INT64,
  bigint_col INT64,
  bool_col BOOL,
  date DATE,
  date_string_col STRING(MAX),
  numeric_col NUMERIC,
  float_col FLOAT64,
  index INT64,
  int_col INT64,
  month INT64,
  smallint_col INT64,
  string_col STRING(MAX),
  timestamp_col TIMESTAMP,
  tinyint_col INT64,
  Unnamed0 INT64,
  year INT64
)
PRIMARY KEY (id);

CREATE TABLE array_table
(
  string_col ARRAY<STRING(MAX)>,
  int_col ARRAY<INT64>,
  id INT64,
)
PRIMARY KEY (id);

CREATE TABLE dvt_core_types (
    id              INT64
,   col_int8        INT64
,   col_int16       INT64
,   col_int32       INT64
,   col_int64       INT64
,   col_dec_20      NUMERIC
,   col_dec_38      NUMERIC
,   col_dec_10_2    NUMERIC
,   col_float32     FLOAT64
,   col_float64     FLOAT64
,   col_varchar_30  STRING(30)
,   col_char_2      STRING(2)
,   col_string      STRING(MAX)
,   col_date        DATE
,   col_datetime    TIMESTAMP
,   col_tstz        TIMESTAMP
) PRIMARY KEY (id);

CREATE VIEW dvt_core_types_vw
SQL SECURITY DEFINER
AS
SELECT t.id,t.col_int8,t.col_int16,t.col_int32,t.col_int64
,t.col_dec_20,t.col_dec_38,t.col_dec_10_2,t.col_float32
,t.col_float64,t.col_varchar_30,t.col_char_2,t.col_string
,t.col_date,t.col_datetime,t.col_tstz
FROM dvt_core_types AS t;

--Integration test table used to test both binary pk matching and binary hash/concat comparisons.
CREATE TABLE dvt_binary
(   binary_id       BYTES(MAX) NOT NULL
,   int_id          INT64 NOT NULL
,   other_data      STRING(100)
)PRIMARY KEY (binary_id);

CREATE TABLE dvt_many_cols
( id INT64
, col_001 STRING(10)
, col_002 STRING(10)
, col_003 STRING(10)
, col_004 STRING(10)
, col_005 STRING(10)
, col_006 STRING(10)
, col_007 STRING(10)
, col_008 STRING(10)
, col_009 STRING(10)
, col_010 STRING(10)
, col_011 INT64
, col_012 INT64
, col_013 INT64
, col_014 INT64
, col_015 INT64
, col_016 INT64
, col_017 INT64
, col_018 INT64
, col_019 INT64
, col_020 INT64
, col_021 STRING(10)
, col_022 STRING(10)
, col_023 STRING(10)
, col_024 STRING(10)
, col_025 STRING(10)
, col_026 STRING(10)
, col_027 STRING(10)
, col_028 STRING(10)
, col_029 STRING(10)
, col_030 STRING(10)
, col_031 INT64
, col_032 INT64
, col_033 INT64
, col_034 INT64
, col_035 INT64
, col_036 INT64
, col_037 INT64
, col_038 INT64
, col_039 INT64
, col_040 INT64
, col_041 STRING(10)
, col_042 STRING(10)
, col_043 STRING(10)
, col_044 STRING(10)
, col_045 STRING(10)
, col_046 STRING(10)
, col_047 STRING(10)
, col_048 STRING(10)
, col_049 STRING(10)
, col_050 STRING(10)
, col_051 INT64
, col_052 INT64
, col_053 INT64
, col_054 INT64
, col_055 INT64
, col_056 INT64
, col_057 INT64
, col_058 INT64
, col_059 INT64
, col_060 INT64
, col_061 STRING(10)
, col_062 STRING(10)
, col_063 STRING(10)
, col_064 STRING(10)
, col_065 STRING(10)
, col_066 STRING(10)
, col_067 STRING(10)
, col_068 STRING(10)
, col_069 STRING(10)
, col_070 STRING(10)
, col_071 INT64
, col_072 INT64
, col_073 INT64
, col_074 INT64
, col_075 INT64
, col_076 INT64
, col_077 INT64
, col_078 INT64
, col_079 INT64
, col_080 INT64
, col_081 STRING(10)
, col_082 STRING(10)
, col_083 STRING(10)
, col_084 STRING(10)
, col_085 STRING(10)
, col_086 STRING(10)
, col_087 STRING(10)
, col_088 STRING(10)
, col_089 STRING(10)
, col_090 STRING(10)
, col_091 INT64
, col_092 INT64
, col_093 INT64
, col_094 INT64
, col_095 INT64
, col_096 INT64
, col_097 INT64
, col_098 INT64
, col_099 INT64
, col_100 INT64
, col_101 STRING(10)
, col_102 STRING(10)
, col_103 STRING(10)
, col_104 STRING(10)
, col_105 STRING(10)
, col_106 STRING(10)
, col_107 STRING(10)
, col_108 STRING(10)
, col_109 STRING(10)
, col_110 STRING(10)
, col_111 INT64
, col_112 INT64
, col_113 INT64
, col_114 INT64
, col_115 INT64
, col_116 INT64
, col_117 INT64
, col_118 INT64
, col_119 INT64
, col_120 INT64
, col_121 STRING(10)
, col_122 STRING(10)
, col_123 STRING(10)
, col_124 STRING(10)
, col_125 STRING(10)
, col_126 STRING(10)
, col_127 STRING(10)
, col_128 STRING(10)
, col_129 STRING(10)
, col_130 STRING(10)
, col_131 INT64
, col_132 INT64
, col_133 INT64
, col_134 INT64
, col_135 INT64
, col_136 INT64
, col_137 INT64
, col_138 INT64
, col_139 INT64
, col_140 INT64
, col_141 STRING(10)
, col_142 STRING(10)
, col_143 STRING(10)
, col_144 STRING(10)
, col_145 STRING(10)
, col_146 STRING(10)
, col_147 STRING(10)
, col_148 STRING(10)
, col_149 STRING(10)
, col_150 STRING(10)
, col_151 INT64
, col_152 INT64
, col_153 INT64
, col_154 INT64
, col_155 INT64
, col_156 INT64
, col_157 INT64
, col_158 INT64
, col_159 INT64
, col_160 INT64
, col_161 STRING(10)
, col_162 STRING(10)
, col_163 STRING(10)
, col_164 STRING(10)
, col_165 STRING(10)
, col_166 STRING(10)
, col_167 STRING(10)
, col_168 STRING(10)
, col_169 STRING(10)
, col_170 STRING(10)
, col_171 INT64
, col_172 INT64
, col_173 INT64
, col_174 INT64
, col_175 INT64
, col_176 INT64
, col_177 INT64
, col_178 INT64
, col_179 INT64
, col_180 INT64
, col_181 STRING(10)
, col_182 STRING(10)
, col_183 STRING(10)
, col_184 STRING(10)
, col_185 STRING(10)
, col_186 STRING(10)
, col_187 STRING(10)
, col_188 STRING(10)
, col_189 STRING(10)
, col_190 STRING(10)
, col_191 INT64
, col_192 INT64
, col_193 INT64
, col_194 INT64
, col_195 INT64
, col_196 INT64
, col_197 INT64
, col_198 INT64
, col_199 INT64
, col_200 INT64
, col_201 STRING(10)
, col_202 STRING(10)
, col_203 STRING(10)
, col_204 STRING(10)
, col_205 STRING(10)
, col_206 STRING(10)
, col_207 STRING(10)
, col_208 STRING(10)
, col_209 STRING(10)
, col_210 STRING(10)
, col_211 INT64
, col_212 INT64
, col_213 INT64
, col_214 INT64
, col_215 INT64
, col_216 INT64
, col_217 INT64
, col_218 INT64
, col_219 INT64
, col_220 INT64
, col_221 STRING(10)
, col_222 STRING(10)
, col_223 STRING(10)
, col_224 STRING(10)
, col_225 STRING(10)
, col_226 STRING(10)
, col_227 STRING(10)
, col_228 STRING(10)
, col_229 STRING(10)
, col_230 STRING(10)
, col_231 INT64
, col_232 INT64
, col_233 INT64
, col_234 INT64
, col_235 INT64
, col_236 INT64
, col_237 INT64
, col_238 INT64
, col_239 INT64
, col_240 INT64
, col_241 STRING(10)
, col_242 STRING(10)
, col_243 STRING(10)
, col_244 STRING(10)
, col_245 STRING(10)
, col_246 STRING(10)
, col_247 STRING(10)
, col_248 STRING(10)
, col_249 STRING(10)
, col_250 STRING(10)
, col_251 INT64
, col_252 INT64
, col_253 INT64
, col_254 INT64
, col_255 INT64
, col_256 INT64
, col_257 INT64
, col_258 INT64
, col_259 INT64
, col_260 INT64
, col_261 STRING(10)
, col_262 STRING(10)
, col_263 STRING(10)
, col_264 STRING(10)
, col_265 STRING(10)
, col_266 STRING(10)
, col_267 STRING(10)
, col_268 STRING(10)
, col_269 STRING(10)
, col_270 STRING(10)
, col_271 INT64
, col_272 INT64
, col_273 INT64
, col_274 INT64
, col_275 INT64
, col_276 INT64
, col_277 INT64
, col_278 INT64
, col_279 INT64
, col_280 INT64
, col_281 STRING(10)
, col_282 STRING(10)
, col_283 STRING(10)
, col_284 STRING(10)
, col_285 STRING(10)
, col_286 STRING(10)
, col_287 STRING(10)
, col_288 STRING(10)
, col_289 STRING(10)
, col_290 STRING(10)
, col_291 INT64
, col_292 INT64
, col_293 INT64
, col_294 INT64
, col_295 INT64
, col_296 INT64
, col_297 INT64
, col_298 INT64
, col_299 INT64
, col_300 INT64
, col_301 STRING(10)
, col_302 STRING(10)
, col_303 STRING(10)
, col_304 STRING(10)
, col_305 STRING(10)
, col_306 STRING(10)
, col_307 STRING(10)
, col_308 STRING(10)
, col_309 STRING(10)
, col_310 STRING(10)
, col_311 INT64
, col_312 INT64
, col_313 INT64
, col_314 INT64
, col_315 INT64
, col_316 INT64
, col_317 INT64
, col_318 INT64
, col_319 INT64
, col_320 INT64
, col_321 STRING(10)
, col_322 STRING(10)
, col_323 STRING(10)
, col_324 STRING(10)
, col_325 STRING(10)
, col_326 STRING(10)
, col_327 STRING(10)
, col_328 STRING(10)
, col_329 STRING(10)
, col_330 STRING(10)
, col_331 INT64
, col_332 INT64
, col_333 INT64
, col_334 INT64
, col_335 INT64
, col_336 INT64
, col_337 INT64
, col_338 INT64
, col_339 INT64
, col_340 INT64
, col_341 STRING(10)
, col_342 STRING(10)
, col_343 STRING(10)
, col_344 STRING(10)
, col_345 STRING(10)
, col_346 STRING(10)
, col_347 STRING(10)
, col_348 STRING(10)
, col_349 STRING(10)
, col_350 STRING(10)
, col_351 INT64
, col_352 INT64
, col_353 INT64
, col_354 INT64
, col_355 INT64
, col_356 INT64
, col_357 INT64
, col_358 INT64
, col_359 INT64
, col_360 INT64
, col_361 STRING(10)
, col_362 STRING(10)
, col_363 STRING(10)
, col_364 STRING(10)
, col_365 STRING(10)
, col_366 STRING(10)
, col_367 STRING(10)
, col_368 STRING(10)
, col_369 STRING(10)
, col_370 STRING(10)
, col_371 INT64
, col_372 INT64
, col_373 INT64
, col_374 INT64
, col_375 INT64
, col_376 INT64
, col_377 INT64
, col_378 INT64
, col_379 INT64
, col_380 INT64
, col_381 STRING(10)
, col_382 STRING(10)
, col_383 STRING(10)
, col_384 STRING(10)
, col_385 STRING(10)
, col_386 STRING(10)
, col_387 STRING(10)
, col_388 STRING(10)
, col_389 STRING(10)
, col_390 STRING(10)
, col_391 INT64
, col_392 INT64
, col_393 INT64
, col_394 INT64
, col_395 INT64
, col_396 INT64
, col_397 INT64
, col_398 INT64
, col_399 INT64
) PRIMARY KEY (id);

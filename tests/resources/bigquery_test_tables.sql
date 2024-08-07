
-- BigQuery table with several different numeric datatypes with the same value
CREATE OR REPLACE TABLE pso_data_validator.test_data_types AS
SELECT
    CAST('1234567890123456789012345678901234.00' AS BIGNUMERIC) bignumeric_type,
    CAST(2 AS INT64) int_type,
    CAST(2 AS DECIMAL) decimal_type,
    CAST(2 AS STRING) text_type,
    CAST('2021-01-01 00:00:00' AS TIMESTAMP) timestamp_type


-- Core data types test table, to be kept in sync with same table in other SQL engines
CREATE OR REPLACE TABLE `pso_data_validator`.`dvt_core_types`
(   id              INT NOT NULL
,   col_int8        TINYINT
,   col_int16       SMALLINT
,   col_int32       INT
,   col_int64       INT64
,   col_dec_20      NUMERIC(20)
,   col_dec_38      BIGNUMERIC(38)
,   col_dec_10_2    NUMERIC(10,2)
,   col_float32     FLOAT64
,   col_float64     FLOAT64
,   col_varchar_30  STRING(30)
,   col_char_2      STRING(2)
,   col_string      STRING
,   col_date        DATE
,   col_datetime    DATETIME
,   col_tstz        TIMESTAMP
) OPTIONS (description='Core data types integration test table');

INSERT INTO `pso_data_validator`.`dvt_core_types` VALUES
(1,1,1,1,1
 ,NUMERIC '12345678901234567890'
 ,BIGNUMERIC '1234567890123456789012345'
 ,NUMERIC '123.11',123456.1,12345678.1
 ,'Hello DVT','A ','Hello DVT'
 ,DATE '1970-01-01',DATETIME '1970-01-01 00:00:01'
 ,TIMESTAMP '1970-01-01 00:00:01-01:00')
,(2,2,2,2,2
 ,NUMERIC '12345678901234567890'
 ,BIGNUMERIC '1234567890123456789012345'
 ,NUMERIC '123.22',123456.2,12345678.2
 ,'Hello DVT','B ','Hello DVT'
 ,DATE '1970-01-02',DATETIME '1970-01-02 00:00:02'
 ,TIMESTAMP '1970-01-02 00:00:02-02:00')
,(3,3,3,3,3
 ,NUMERIC '12345678901234567890'
 ,BIGNUMERIC '1234567890123456789012345'
 ,NUMERIC '123.3',123456.3,12345678.3
 ,'Hello DVT','C ','Hello DVT'
 ,DATE '1970-01-03',DATETIME '1970-01-03 00:00:03'
 ,TIMESTAMP '1970-01-03 00:00:03-03:00');

DROP TABLE `pso_data_validator`.`dvt_null_not_null`;
CREATE TABLE `pso_data_validator`.`dvt_null_not_null`
(   col_nn             DATETIME NOT NULL
,   col_nullable       DATETIME
,   col_src_nn_trg_n   DATETIME
,   col_src_n_trg_nn   DATETIME NOT NULL
) OPTIONS (description='Nullable integration test table, BigQuery is assumed to be a DVT target (not source)');

DROP TABLE `pso_data_validator`.`dvt_large_decimals`;
CREATE TABLE `pso_data_validator`.`dvt_large_decimals`
(   id              BIGNUMERIC(38) NOT NULL
,   col_data        STRING(10)
,   col_dec_18      INT64
,   col_dec_38      BIGNUMERIC(38)
,   col_dec_38_9    NUMERIC
,   col_dec_38_30   BIGNUMERIC(38,30)
) OPTIONS (description='Large decimals integration test table');

INSERT INTO `pso_data_validator`.`dvt_large_decimals` VALUES
(BIGNUMERIC '123456789012345678901234567890'
,'Row 1'
,123456789012345678
,BIGNUMERIC '12345678901234567890123456789012345678'
,NUMERIC '12345678901234567890123456789.123456789'
,BIGNUMERIC '12345678.123456789012345678901234567890')
,(BIGNUMERIC '223456789012345678901234567890'
,'Row 2'
,223456789012345678
,BIGNUMERIC '22345678901234567890123456789012345678'
,NUMERIC '22345678901234567890123456789.123456789'
,BIGNUMERIC '22345678.123456789012345678901234567890')
,(BIGNUMERIC '323456789012345678901234567890'
,'Row 3'
,323456789012345678
,BIGNUMERIC '32345678901234567890123456789012345678'
,NUMERIC '32345678901234567890123456789.123456789'
,BIGNUMERIC '32345678.123456789012345678901234567890');

DROP TABLE `pso_data_validator`.`dvt_binary`;
CREATE TABLE `pso_data_validator`.`dvt_binary`
(   binary_id       BYTES(16) NOT NULL
,   int_id          INT64 NOT NULL
,   other_data      STRING(100)
) OPTIONS (description='Integration test table used to test both binary pk matching and binary hash/concat comparisons.');
INSERT INTO `pso_data_validator`.`dvt_binary` VALUES
(CAST('DVT-key-1' AS BYTES), 1, 'Row 1'),
(CAST('DVT-key-2' AS BYTES), 2, 'Row 2'),
(CAST('DVT-key-3' AS BYTES), 3, 'Row 3'),
(CAST('DVT-key-4' AS BYTES), 4, 'Row 4'),
(CAST('DVT-key-5' AS BYTES), 5, 'Row 5');

DROP TABLE `pso_data_validator`.`dvt_string_id`;
CREATE TABLE `pso_data_validator`.`dvt_string_id`
(   id          STRING(15) NOT NULL
,   other_data  STRING(100)
) OPTIONS (description='Integration test table used to test string pk matching.');
INSERT INTO `pso_data_validator`.`dvt_string_id` VALUES
('DVT-key-1', 'Row 1'),
('DVT-key-2', 'Row 2'),
('DVT-key-3', 'Row 3'),
('DVT-key-4', 'Row 4'),
('DVT-key-5', 'Row 5');

DROP TABLE `pso_data_validator`.`dvt_char_id`;
-- BigQuery does not have a specific padded CHAR data type.
CREATE TABLE `pso_data_validator`.`dvt_char_id`
(   id          STRING(6) NOT NULL
,   other_data  STRING(100)
) OPTIONS (description='Integration test table used to test CHAR pk matching.');
INSERT INTO `pso_data_validator`.`dvt_char_id` VALUES
('DVT1  ', 'Row 1'),
('DVT2  ', 'Row 2'),
('DVT3  ', 'Row 3'),
('DVT4  ', 'Row 4'),
('DVT5  ', 'Row 5');

DROP TABLE `pso_data_validator`.`dvt_pangrams`;
CREATE TABLE `pso_data_validator`.`dvt_pangrams`
(   id          INT64
,   lang        STRING(100)
,   words       STRING(1000)
,   words_en    STRING(1000)
) OPTIONS (description='Integration test table used to test unicode characters.');
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

DROP TABLE `pso_data_validator`.`dvt_many_cols`;
CREATE TABLE `pso_data_validator`.`dvt_many_cols`
( id INT64
, col_001 STRING
, col_002 STRING
, col_003 STRING
, col_004 STRING
, col_005 STRING
, col_006 STRING
, col_007 STRING
, col_008 STRING
, col_009 STRING
, col_010 STRING
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
, col_021 STRING
, col_022 STRING
, col_023 STRING
, col_024 STRING
, col_025 STRING
, col_026 STRING
, col_027 STRING
, col_028 STRING
, col_029 STRING
, col_030 STRING
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
, col_041 STRING
, col_042 STRING
, col_043 STRING
, col_044 STRING
, col_045 STRING
, col_046 STRING
, col_047 STRING
, col_048 STRING
, col_049 STRING
, col_050 STRING
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
, col_061 STRING
, col_062 STRING
, col_063 STRING
, col_064 STRING
, col_065 STRING
, col_066 STRING
, col_067 STRING
, col_068 STRING
, col_069 STRING
, col_070 STRING
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
, col_081 STRING
, col_082 STRING
, col_083 STRING
, col_084 STRING
, col_085 STRING
, col_086 STRING
, col_087 STRING
, col_088 STRING
, col_089 STRING
, col_090 STRING
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
, col_101 STRING
, col_102 STRING
, col_103 STRING
, col_104 STRING
, col_105 STRING
, col_106 STRING
, col_107 STRING
, col_108 STRING
, col_109 STRING
, col_110 STRING
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
, col_121 STRING
, col_122 STRING
, col_123 STRING
, col_124 STRING
, col_125 STRING
, col_126 STRING
, col_127 STRING
, col_128 STRING
, col_129 STRING
, col_130 STRING
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
, col_141 STRING
, col_142 STRING
, col_143 STRING
, col_144 STRING
, col_145 STRING
, col_146 STRING
, col_147 STRING
, col_148 STRING
, col_149 STRING
, col_150 STRING
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
, col_161 STRING
, col_162 STRING
, col_163 STRING
, col_164 STRING
, col_165 STRING
, col_166 STRING
, col_167 STRING
, col_168 STRING
, col_169 STRING
, col_170 STRING
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
, col_181 STRING
, col_182 STRING
, col_183 STRING
, col_184 STRING
, col_185 STRING
, col_186 STRING
, col_187 STRING
, col_188 STRING
, col_189 STRING
, col_190 STRING
, col_191 INT64
, col_192 INT64
, col_193 INT64
, col_194 INT64
, col_195 INT64
, col_196 INT64
, col_197 INT64
, col_198 INT64
, col_199 INT64
) OPTIONS (description='Integration test table used to test validating many columns.');
INSERT INTO `pso_data_validator`.`dvt_many_cols` (id) VALUES (1);

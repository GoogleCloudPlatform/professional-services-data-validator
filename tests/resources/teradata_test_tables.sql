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

DROP TABLE udf.dvt_core_types;
CREATE TABLE udf.dvt_core_types
(   id              BIGINT NOT NULL PRIMARY KEY
,   col_int8        BYTEINT
,   col_int16       SMALLINT
,   col_int32       INT
,   col_int64       BIGINT
,   col_dec_20      NUMBER(20)
,   col_dec_38      NUMBER(38)
,   col_dec_10_2    NUMBER(10,2)
,   col_float32     REAL
,   col_float64     DOUBLE PRECISION
,   col_varchar_30  VARCHAR(30)
,   col_char_2      CHAR(2)
,   col_string      LONG VARCHAR
,   col_date        DATE
,   col_datetime    TIMESTAMP(3)
,   col_tstz        TIMESTAMP(3) WITH TIME ZONE
);
COMMENT ON TABLE udf.dvt_core_types AS 'Core data types integration test table';

INSERT INTO udf.dvt_core_types VALUES
(1,1,1,1,1
,12345678901234567890,1234567890123456789012345,123.11,123456.1,12345678.1
,'Hello DVT','A ','Hello DVT'
,DATE'1970-01-01',TIMESTAMP'1970-01-01 00:00:01'
,CAST('1970-01-01 00:00:01.000-01:00' AS TIMESTAMP(3) WITH TIME ZONE));
INSERT INTO udf.dvt_core_types VALUES
(2,2,2,2,2
,12345678901234567890,1234567890123456789012345,123.22,123456.2,12345678.2
,'Hello DVT','B ','Hello DVT'
,DATE'1970-01-02',TIMESTAMP'1970-01-02 00:00:02'
,CAST('1970-01-02 00:00:02.000-02:00' AS TIMESTAMP(3) WITH TIME ZONE));
INSERT INTO udf.dvt_core_types VALUES
(3,3,3,3,3
,12345678901234567890,1234567890123456789012345,123.3,123456.3,12345678.3
,'Hello DVT','C ','Hello DVT'
,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:03'
,CAST('1970-01-03 00:00:03.000-03:00' AS TIMESTAMP(3) WITH TIME ZONE));

DROP TABLE udf.dvt_null_not_null;
CREATE TABLE udf.dvt_null_not_null
(   col_nn             TIMESTAMP(0) NOT NULL
,   col_nullable       TIMESTAMP(0)
,   col_src_nn_trg_n   TIMESTAMP(0) NOT NULL
,   col_src_n_trg_nn   TIMESTAMP(0)
);
COMMENT ON TABLE udf.dvt_null_not_null AS 'Nullable integration test table, Teradata is assumed to be a DVT source (not target).';

DROP TABLE udf.dvt_large_decimals;
CREATE TABLE udf.dvt_large_decimals
(   id              NUMBER(38) NOT NULL PRIMARY KEY
,   col_data        VARCHAR(10)
,   col_dec_18      NUMBER(18)
,   col_dec_38      NUMBER(38)
,   col_dec_38_9    NUMBER(38,9)
,   col_dec_38_30   NUMBER(38,30)
);
COMMENT ON TABLE udf.dvt_large_decimals IS 'Large decimals integration test table';

INSERT INTO udf.dvt_large_decimals VALUES
(123456789012345678901234567890
,'Row 1'
,123456789012345678
,12345678901234567890123456789012345678
,12345678901234567890123456789.123456789
,12345678.123456789012345678901234567890);
INSERT INTO udf.dvt_large_decimals VALUES
(223456789012345678901234567890
,'Row 2'
,223456789012345678
,22345678901234567890123456789012345678
,22345678901234567890123456789.123456789
,22345678.123456789012345678901234567890);
INSERT INTO udf.dvt_large_decimals VALUES
(323456789012345678901234567890
,'Row 3'
,323456789012345678
,32345678901234567890123456789012345678
,32345678901234567890123456789.123456789
,32345678.123456789012345678901234567890);

DROP TABLE udf.dvt_binary;
CREATE TABLE udf.dvt_binary
(   binary_id       VARBYTE(16) NOT NULL PRIMARY KEY
,   int_id          NUMBER(10) NOT NULL
,   other_data      VARCHAR(100)
);
COMMENT ON TABLE udf.dvt_binary IS 'Integration test table used to test both binary pk matching and binary hash/concat comparisons.';
INSERT INTO udf.dvt_binary VALUES (TO_BYTES('DVT-key-1','ascii'), 1, 'Row 1');
INSERT INTO udf.dvt_binary VALUES (TO_BYTES('DVT-key-2','ascii'), 2, 'Row 2');
INSERT INTO udf.dvt_binary VALUES (TO_BYTES('DVT-key-3','ascii'), 3, 'Row 3');
INSERT INTO udf.dvt_binary VALUES (TO_BYTES('DVT-key-4','ascii'), 4, 'Row 4');
INSERT INTO udf.dvt_binary VALUES (TO_BYTES('DVT-key-5','ascii'), 5, 'Row 5');

DROP TABLE udf.dvt_string_id;
CREATE TABLE udf.dvt_string_id
(   id          VARCHAR(15) NOT NULL PRIMARY KEY
,   other_data  VARCHAR(100)
);
COMMENT ON TABLE udf.dvt_string_id IS 'Integration test table used to test string pk matching.';
INSERT INTO udf.dvt_string_id VALUES ('DVT-key-1', 'Row 1');
INSERT INTO udf.dvt_string_id VALUES ('DVT-key-2', 'Row 2');
INSERT INTO udf.dvt_string_id VALUES ('DVT-key-3', 'Row 3');
INSERT INTO udf.dvt_string_id VALUES ('DVT-key-4', 'Row 4');
INSERT INTO udf.dvt_string_id VALUES ('DVT-key-5', 'Row 5');

DROP TABLE udf.dvt_char_id;
CREATE TABLE udf.dvt_char_id
(   id          CHAR(6) NOT NULL PRIMARY KEY
,   other_data  VARCHAR(100)
);
COMMENT ON TABLE udf.dvt_char_id IS 'Integration test table used to test CHAR pk matching.';
INSERT INTO udf.dvt_char_id VALUES ('DVT1', 'Row 1');
INSERT INTO udf.dvt_char_id VALUES ('DVT2', 'Row 2');
INSERT INTO udf.dvt_char_id VALUES ('DVT3', 'Row 3');
INSERT INTO udf.dvt_char_id VALUES ('DVT4', 'Row 4');
INSERT INTO udf.dvt_char_id VALUES ('DVT5', 'Row 5');

DROP TABLE udf.dvt_pangrams;
CREATE TABLE udf.dvt_pangrams
(   id          NUMBER(5) NOT NULL PRIMARY KEY
,   lang        VARCHAR(100)
,   words       VARCHAR(1000) CHARACTER SET UNICODE
,   words_en    VARCHAR(1000)
);
COMMENT ON TABLE udf.dvt_pangrams IS 'Integration test table used to test unicode characters.';
-- Text taken from Wikipedia, we cannot guarantee translations :-)
-- Ensure to load data in utf8 mode: bteq -c utf8
INSERT INTO udf.dvt_pangrams
VALUES (1,'Hebrew',
        'שפן אכל קצת גזר בטעם חסה, ודי',
        'A bunny ate some lettuce-flavored carrots, and he had enough');
INSERT INTO udf.dvt_pangrams
VALUES (2,'Polish',
        'Pchnąć w tę łódź jeża lub ośm skrzyń fig',
        'Push a hedgehog or eight crates of figs in this boat');
INSERT INTO udf.dvt_pangrams
VALUES (3,'Russian',
        'Съешь ещё этих мягких французских булок, да выпей же чаю',
        'Eat more of these soft French loaves and drink a tea');
INSERT INTO udf.dvt_pangrams
VALUES (4,'Swedish',
        'Schweiz för lyxfjäder på qvist bakom ugn',
        'Switzerland brings luxury feather on branch behind oven');
INSERT INTO udf.dvt_pangrams
VALUES (5,'Turkish',
        'Pijamalı hasta yağız şoföre çabucak güvendi',
        'The sick person in pyjamas quickly trusted the swarthy driver');

DROP TABLE udf.dvt_many_cols;
CREATE TABLE udf.dvt_many_cols
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
);
COMMENT ON TABLE udf.dvt_many_cols AS 'Integration test table used to test validating many columns.';
INSERT INTO udf.dvt_many_cols (id) VALUES (1);

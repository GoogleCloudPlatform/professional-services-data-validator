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
	ID INT NOT NULL,
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
 ,12345678901234567890,1234567890123456789012345,123.33,123456.3,12345678.3
 ,'Hello DVT','C ','Hello DVT'
 ,DATE'1970-01-03',TIMESTAMP'1970-01-03 00:00:03'
 ,'1970-01-03 00:00:03 -03:00');
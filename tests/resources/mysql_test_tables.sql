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

DROP TABLE `pso_data_validator`.`test_data_types_mysql_row`;
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

DROP TABLE `pso_data_validator`.`dvt_core_types`;
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

DROP TABLE `pso_data_validator`.`dvt_null_not_null`;
CREATE TABLE `pso_data_validator`.`dvt_null_not_null`
(   col_nn             datetime(0) NOT NULL
,   col_nullable       datetime(0)
,   col_src_nn_trg_n   datetime(0) NOT NULL
,   col_src_n_trg_nn   datetime(0)
) COMMENT 'Nullable integration test table, MySQL is assumed to be a DVT source (not target).';

DROP TABLE `pso_data_validator`.`dvt_binary`;
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

DROP TABLE `pso_data_validator`.`dvt_char_id`;
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

DROP TABLE `pso_data_validator`.`dvt_pangrams`;
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

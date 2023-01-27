-- Copyright 2020 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

DROP TABLE IF EXISTS test_data_types_mysql_row;
CREATE TABLE test_data_types_mysql_row(
	serial_col int AUTO_INCREMENT ,
    int_col int(3),
	text_col  char(100),
    char_col  char(30),
    varchar_col  varchar(255),
	float_col float(5,2),
	decimal_col  decimal(5, 2),
	datetime_col datetime,
    date_col date,
	PRIMARY KEY (serial_col)
);
INSERT INTO test_data_types_mysql_row(int_col, text_col, char_col, varchar_col, float_col, decimal_col, datetime_col, date_col) values (10, 'row1_text', '1', 'row1_varchar', 10.10, 10.101, '2016-06-22 19:10:25','2020-01-01');
INSERT INTO test_data_types_mysql_row(int_col, text_col, char_col, varchar_col, float_col, decimal_col, datetime_col, date_col) values (20, 'row2_text', '2', 'row2_varchar', 20.20, 20.202, '2016-06-22 19:20:25','2020-02-02');
INSERT INTO test_data_types_mysql_row(int_col, text_col, char_col, varchar_col, float_col, decimal_col, datetime_col, date_col) values (30, 'row3_text', '3', 'row3_varchar', 30.20, 30.203, '2016-06-22 19:30:25','2020-03-03');
INSERT INTO test_data_types_mysql_row(int_col, text_col, char_col, varchar_col, float_col, decimal_col, datetime_col, date_col) values (40, 'row4_text', '4', 'row4_varchar', 40.20, 40.404, '2016-06-22 19:30:45','2020-04-04');
INSERT INTO test_data_types_mysql_row(int_col, text_col, char_col, varchar_col, float_col, decimal_col, datetime_col, date_col) values (50, 'row5_text', '5', 'row5_varchar', 50.20, 50.404, '2016-06-22 19:50:44','2020-05-05');
INSERT INTO test_data_types_mysql_row(int_col, text_col, char_col, varchar_col, float_col, decimal_col, datetime_col, date_col) values (60, 'row6_text', '6', 'row6_varchar', 60.20, 60.404, '2016-01-22 19:51:44','2020-06-06');
INSERT INTO test_data_types_mysql_row(int_col, text_col, char_col, varchar_col, float_col, decimal_col, datetime_col, date_col) values (70, 'row7_text', '7', 'row7_varchar', 70.20, 70.404, '2017-01-22 19:51:44','2020-06-07');
INSERT INTO test_data_types_mysql_row(int_col, text_col, char_col, varchar_col, float_col, decimal_col, datetime_col, date_col) values (80, 'row8_text', '8', 'row8_varchar', 80.20, 80.404, '2018-01-22 19:51:44','2020-06-08');
INSERT INTO test_data_types_mysql_row(int_col, text_col, char_col, varchar_col, float_col, decimal_col, datetime_col, date_col) values (90, 'row9_text', '9', 'row9_varchar', 90.20, 90.404, '2019-01-22 19:51:44','2020-06-09');
INSERT INTO test_data_types_mysql_row(int_col, text_col, char_col, varchar_col, float_col, decimal_col, datetime_col, date_col) values (100, 'row10_text', '1', 'row10_varchar', 100.20, 100.404, '2020-01-22 19:51:44','2021-06-09');
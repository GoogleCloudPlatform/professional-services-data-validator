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

INSERT INTO students_pointer
  (id,name,division,marks,exam,overall_pointer,date_of_exam)
VALUES(101, 'Ross', 12, 500, 'Biology', 9.8, '2002-02-10 15:30:00+00');

INSERT INTO students_pointer
  (id,name,division,marks,exam,overall_pointer,date_of_exam)
VALUES(102, 'Rachel', 14, 460, 'Chemistry', 9.9, '2018-04-22');

INSERT INTO students_pointer
  (id,name,division,marks,exam,overall_pointer,date_of_exam)
VALUES(103, 'Chandler', 12, 480, 'Biology', 8.2, '2016-04-14');

INSERT INTO students_pointer
  (id,name,division,marks,exam,overall_pointer,date_of_exam)
VALUES(104, 'Monica', 12, 390, 'Maths', 9.2, '2019-04-29');

INSERT INTO students_pointer
  (id,name,division,marks,exam,overall_pointer,date_of_exam)
VALUES(105, 'Joey', 16, 410, 'Maths', 9.7, '2019-06-21');

INSERT INTO students_pointer
  (id,name,division,marks,exam,overall_pointer,date_of_exam)
VALUES(106, 'Phoebe', 10, 490, 'Chemistry', 9.6, '2019-02-09');


INSERT INTO functional_alltypes
  (id ,bigint_col ,bool_col ,date ,date_string_col ,numeric_col ,float_col ,index ,int_col ,month ,smallint_col ,string_col ,timestamp_col ,tinyint_col ,Unnamed0 ,year )
VALUES
  (1, 10001, TRUE, '2016-02-09', '01/01/2001', 2.5, 12.16, 101, 21, 4, 16, 'David', '2002-02-10 15:30:00+00', 6, 99, 2010);

INSERT INTO functional_alltypes
  (id ,bigint_col ,bool_col ,date ,date_string_col ,numeric_col ,float_col ,index ,int_col ,month ,smallint_col ,string_col ,timestamp_col ,tinyint_col ,Unnamed0 ,year )
VALUES
  (2, 10002, FALSE, '2016-10-10', '02/02/2002', 2.6, 13.16, 102, 22, 5, 18, 'Ryan', '2009-02-12 10:06:00+00', 7, 98, 2012);

INSERT INTO functional_alltypes
  (id ,bigint_col ,bool_col ,date ,date_string_col ,numeric_col ,float_col ,index ,int_col ,month ,smallint_col ,string_col ,timestamp_col ,tinyint_col ,Unnamed0 ,year )
VALUES
  (3, 10003, TRUE, '2018-02-09', '03/03/2003', 9.5, 44.16, 201, 41, 6, 56, 'Steve', '2010-06-10 12:12:00+00', 12, 66, 2006);

INSERT INTO functional_alltypes
  (id ,bigint_col ,bool_col ,date ,date_string_col ,numeric_col ,float_col ,index ,int_col ,month ,smallint_col ,string_col ,timestamp_col ,tinyint_col ,Unnamed0 ,year )
VALUES
  (4, 10004, TRUE, '2018-10-10', '04/04/2004', 9.6, 45.16, 202, 42, 9, 58, 'Chandler', '2014-06-12 10:04:00+00', 14, 69, 2009);

INSERT INTO functional_alltypes
  (id ,bigint_col ,bool_col ,date ,date_string_col ,numeric_col ,float_col ,index ,int_col ,month ,smallint_col ,string_col ,timestamp_col ,tinyint_col ,Unnamed0 ,year )
VALUES
  (5, 10005, FALSE, '2020-06-12', '05/05/2005', 6.6, 66.12, 401, 62, 12, 98, 'Rose', '2018-02-10 10:06:00+00', 16, 96, 2012);

INSERT INTO functional_alltypes
  (id ,bigint_col ,bool_col ,date ,date_string_col ,numeric_col ,float_col ,index ,int_col ,month ,smallint_col ,string_col ,timestamp_col ,tinyint_col ,Unnamed0 ,year )
VALUES
  (6, 10006, TRUE, '2020-12-12', '06/06/2006', 6.9, 66.19, 402, 69, 14, 99, 'Rachel', '2019-04-12 12:09:00+00', 18, 99, 2014);

INSERT INTO array_table
  (id,string_col,int_col)
VALUES
  (1, ['Peter','David'], [11,12]);

INSERT INTO array_table
  (id,string_col,int_col)
VALUES
  (2, ['Raj','Dev','Neil'], [1,2,3]);

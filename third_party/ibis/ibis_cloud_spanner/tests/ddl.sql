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
  double_col NUMERIC,
  float_col NUMERIC,
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

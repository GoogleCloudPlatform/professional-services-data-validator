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

connect to testdb

DROP TABLE dvt_binary;
CREATE TABLE dvt_binary
(   binary_id       VARBINARY(16) NOT NULL PRIMARY KEY
,   int_id          INTEGER NOT NULL
,   other_data      VARCHAR(100)
);
CREATE UNIQUE INDEX dvt_binary_int_id_uk ON dvt_binary (int_id);
COMMENT ON TABLE dvt_binary IS 'Integration test table used to test both binary pk matching and binary hash/concat comparisons.';
INSERT INTO dvt_binary VALUES (CAST('DVT-key-1' AS VARBINARY(16)), 1, 'Row 1');
INSERT INTO dvt_binary VALUES (CAST('DVT-key-2' AS VARBINARY(16)), 2, 'Row 2');
INSERT INTO dvt_binary VALUES (CAST('DVT-key-3' AS VARBINARY(16)), 3, 'Row 3');
INSERT INTO dvt_binary VALUES (CAST('DVT-key-4' AS VARBINARY(16)), 4, 'Row 4');
INSERT INTO dvt_binary VALUES (CAST('DVT-key-5' AS VARBINARY(16)), 5, 'Row 5');
COMMIT;
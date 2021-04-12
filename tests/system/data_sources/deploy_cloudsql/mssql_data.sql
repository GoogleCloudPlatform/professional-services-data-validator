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

DROP TABLE IF EXISTS entries;
CREATE TABLE entries (
guestName VARCHAR(255),
content VARCHAR(255),
entryID INT NOT NULL IDENTITY PRIMARY KEY
);
INSERT INTO entries(guestName, content) VALUES ('Madeline', 'Arrived');
INSERT INTO entries(guestName, content) values ('Annie', 'Me too!');
INSERT INTO entries(guestName, content) values ('Bob', 'More data coming');
INSERT INTO entries(guestName, content) values ('Joe', 'Me too!');
INSERT INTO entries(guestName, content) values ('John', 'Here!');
INSERT INTO entries(guestName, content) values ('Alex', 'Me too!');
INSERT INTO entries(guestName, content) values ('Zoe', 'Same!');

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

CREATE DATABASE guestbook;
USE guestbook;

DROP TABLE IF EXISTS `entries`;
CREATE TABLE entries (
guestName VARCHAR(255),
content VARCHAR(255),
entryID INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(entryID)
);
INSERT INTO entries (guestName, content) values ("first guest", "I got here!");
INSERT INTO entries (guestName, content) values ("second guest", "Me too!");

CREATE USER 'rep_user'@'%' identified by 'rep_password';
GRANT REPLICATION SLAVE ON *.* TO  'rep_user'@'%';
GRANT ALL PRIVILEGES on *.* to 'rep_user'@'%';
FLUSH PRIVILEGES;


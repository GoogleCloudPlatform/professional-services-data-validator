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

CREATE SCHEMA Sales;

DROP TABLE IF EXISTS Sales.customers;
CREATE TABLE Sales.customers (
customer VARCHAR(255),
content VARCHAR(255),
entryID INT NOT NULL IDENTITY PRIMARY KEY
);
INSERT INTO Sales.customers(customer, content) VALUES ('Madeline', 'Arrived');
INSERT INTO Sales.customers(customer, content) values ('Annie', 'Me too!');
INSERT INTO Sales.customers(customer, content) values ('Bob', 'More data coming');
INSERT INTO Sales.customers(customer, content) values ('Joe', 'Me too!');
INSERT INTO Sales.customers(customer, content) values ('John', 'Here!');
INSERT INTO Sales.customers(customer, content) values ('Alex', 'Me too!');
INSERT INTO Sales.customers(customer, content) values ('Zoe', 'Same!');

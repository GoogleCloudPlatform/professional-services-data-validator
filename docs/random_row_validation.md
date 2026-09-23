# Random Row Validation

Data Validation Tool (DVT) supports validating a subset of random rows across two tables. A set of random rows is selected in the source table and the corresponding rows are selected in the target table. Hashes are generated for each row in the source and target tables and compared. This validates data integrity across two tables quickly with some risk of a false positive.

DVT does this by generating the SQL statement with a WHERE clause such as `primary_key=p1 OR primary_key=p2 OR ...`. Where the underlying engine supports IN operator, DVT will generate `primary_key IN (p1, p2, ...).`, where p1, p2 are randomly generated values. The IN clause is more efficient and usually allows for a larger number of rows to be compared. The maximum number of random rows is different for different database engines. Most database engines cannot support more than a few thousand random rows in this fashion, especially if composite primary keys are involved

## Validating large numbers (10000+) of random rows

Businesses sometimes have to validate a set % of rows in a table with millions of rows. If the requirement is to validate 100000 rows from a table with million rows, DVT does not support this out of the box. However, such a validation is possible if a deterministic sample can be made across both database engines. In this scenario, DVT can be used to compare the two samples row by row. The generate-table-partitions command can be used to scale DVT horizontally if the sample size itself is millions of rows.

### Creating a deterministic sample
A deterministic sample means that the same rows are selected across both database engines every time the sample is generated. This can be achieved by using a combination of primary keys, a random seed and a hash function that is common across both database engines. Most databases support SHA256, which generates a 64 character hex string. A SHA256 hash of (primary_key(s) + seed_value) is guaranteed to be the same across both databases for the same primary key(s) and seed_value.  For example the expression `CAST(CONCAT('0x', LEFT(TO_HEX(SHA256(CONCAT(primary_key, 'seed1'))),10)) as int64)` in BigQuery generates the same integer value as `('0x' || left(encode(sha256((primary_key || 'seed1')::bytea), 'hex'),10))::bigint` in PostgreSQL. The entire hash is too large to fit into an integer type value in BigQuery or Postgres. Here the `primary_key` refers to a string primary key. You can also convert the primary key to a string as long as the same format is used on both engines. If the primary key is a composite key, then the same approach can be used to concatenate the primary key columns in the same order on both engines before hashing.

So, given a table A in BigQuery with columns pk1, pk2, pk3 uniquely identifying a row, a deterministic sample of approx 10% of the rows in the table corresponds to:
```
select * from A where MOD(CAST(CONCAT('0x', LEFT(TO_HEX(SHA256(CONCAT(pk1, pk2, pk3, 'seed1'))),10)) as int64), 10) = 0;
```
The select query guaranteed to return the same rows (if present) from the Postgres table is:
```
SELECT * from A where  ('0x' || left(encode(sha256((pk1 || pk2 || pk3 || 'seed1')::bytea), 'hex'),10))::bigint % 10 = 0;
```
These two custom queries can be validated against each other. Since DVT supports generate-table-partitions on custom queries, the validation can be scaled to validate millions of rows if required. If the tables are being validated multiple times over days or weeks, new sets of rows can be validated by changing the seed parameter. 


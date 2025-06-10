# String Handling by DVT

## Background

String is a common data type and proper handling of strings is critical to data validation. As of Feb 1, 2025, DVT handles most strings types correctly while comparing across databases. There are some situations where DVT's handling is not quite correct, specifically around fixed/variable length character strings, blank padding and character set encoding. This document outlines the different string types and the behavior of the different databases. DVT should have a consistent well thought out mechanism to "correctly" compare strings across different databases.

Most databases - for historical reasons support fixed and variable length character strings. Some modern databases e.g. Snowflake, BigQuery, Spanner only support variable length character strings. Many databases support storing strings in different character sets (USASCII, ISO-8859, Unicode). Some, e.g. BigQuery, Spanner only store strings in a specific character set and encoding (UTF8). In most cases databases allow character manipulation across character encodings with some limitations.

DVT (and Python) default to using variable length character strings encoded in UTF8. When comparisons are made, DVT generates SQL for the source and target databases to convert their strings to UTF8. Depending on the particular data types (fixed vs variable length), the SQL uses cast and trim operators to ensure the output UTF8 bytes are equal when the source and target strings are the same.

## Data types support and behavior by database

| Database | Primary String data types | Secondary (encoding) data types | Comments |
| --- | --- | --- | --- |
| AlloyDB | See Postgres | See Postgres | See Postgres |
| BigQuery | STRING | None | All stored in UTF8 |
| Db2 | CHAR, VARCHAR | | CHAR datatype is padded; Encoding specified per database |
| Hive | STRING, CHAR, VARCHAR | | CHAR datatype is padded; Encoding specified per table |
| Impala | CHAR, VARCHAR | | CHAR datatype is padded; Stored as UTF8 |
| MSSQL | CHAR, VARCHAR | NCHAR, NVARCHAR |CHAR datatype is padded; Encoding for CHAR/VARCHAR specified at server, database, table and column level; Encoding for NCHAR/NVARCHAR limited to UTF16 or UCS-2 at the database level |
| MySQL | CHAR, VARCHAR, TEXT, TINYTEXT | None | Trailing spaces in CHAR columns are trimmed on retrieval; Encoding specified at server, database, table and column level|
| Oracle | CHAR, VARCHAR2 | NCHAR, NVARCHAR2 |CHAR datatype is padded; Oracle does not distinguish between NULL and ''; Encoding for CHAR/VARCHAR based on NLS_CHARACTERSET and NCHAR/NVARCHAR on NLS_NCHAR_CHARACTERSET parameters set at the database level |
| Postgres | char, varchar and text | None |CHAR datatype is padded; Encoding specified at the database or server level |
| Redshift | CHAR, VARCHAR ||CHAR datatype is padded; UTF8 encoding for VARCHAR, ASCII7 for CHAR |
| Snowflake | VARCHAR | | All variants CHAR, NCHAR etc syonymous with VARCHAR, all stored in UTF8 | 
| Spanner | See BigQuery | See BigQuery | See BigQuery |
| Teradata | char, varchar | None |Shorter of two strings is padded during comparison per SQL-92 standard; Encoding and collation specified at the column level |

## Handling fixed and variable length character strings
DVT (Python) strings have variable length. Some databases (BigQuery, Spanner, Snowflake) do not support fixed length strings. Trailing blanks are not semantically significant in fixed length strings and are semantically significant in variable length strings. Therefore to correctly compare a string from a source database (say Postgres) with a string in BigQuery, DVT must remove trailing blanks if the data type is fixed length string. DVT must not remove trailing blanks if the data type is variable length string. An [issue](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1422) has been opened.

Fortunately, most databases trim trailing blanks when a fixed length string is cast to a variable length string. So the statement SELECT CAST(CAST('ABCD' AS CHAR(10)) AS VARCHAR(10)) produces 'ABCD'. Therefore in most databases, DVT can cast a string column to VARCHAR without considering whether the string column is of fixed length (and needs trailing blanks removed) or variable length (no character removal required). The database will return the correct result.  

There are a few databases (Db2, Microsoft SQL Server and Oracle) where trailing blanks are not removed when a fixed length string is cast to variable length string. That is, the above select statement returns a string with trailing blanks. For these databases, DVT must identify the type of the column and trim trailing blanks only if the data type is a fixed length string. Variable length strings must not be trimmed, or it can produce incorrect results.

## Handling ASCII, Latin-1 and Unicode character sets

The DVT test suite has tests for Latin and Asian characters (dvt-pangrams) represented in UTF-8 for all databases except Db2. See [issue](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1425). These tests pass. Therefore DVT correctly processes string data stored in UTF-8 across the databases.

For databases where the encoding can only be set at the database or server level (Oracle, Db2 and Postgres) we need to test with a database with a different encoding (e.g. ISO-8859-1), store Latin characters in it and compare it against the same strings stored in BigQuery. A separate [issue](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1423) has been opened.

For Hive, MySQl and Microsoft SQL Server the encoding can be set at the table level, create a table with ISO-8859-1 encoding, store Latin characters in it and compare against the same strings stored in BigQuery. A separate [issue](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1424) has been opened.

DVT correctly handles Teradata strings encoded in ISO-8859 (Latin) encoding [PR 1226](https://github.com/GoogleCloudPlatform/professional-services-data-validator/pull/1226)

## Handling NCHAR and NVARCHAR data types in Microsoft SQL Server and Oracle

Customer has opened an [issue](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1406) that tables with NCHAR and NVARCHAR cannot be validated in Oracle. It is possible a similar issue also exists in SQL Server. By definition, NCHAR and NVARCHAR use Unicode characters and in theory is a superset of the encoding used for CHAR and VARCHAR. In practice, current versions of Oracle and SQL Server use UTF8 encoding for CHAR and VARCHAR by default. Customers upgrading from earlier versions may be using a more limited encoding such as ASCII7 or Latin-1.

The character set for NCHAR and NVARCHAR (Unicode) is guaranteed to be a superset of the character set for CHAR and VARCHAR. One approach to validate CHAR and NCHAR strings is to convert both types of strings to NCHAR (or NVARCHAR). These could then be encoded in UTF8 and compared across database backends. Please note comments earlier in this document that fixed length strings for Oracle and SQL Server must be blank trimmed prior to processing. Once converted to variable length character strings in Unicode, DVT can perform the comparison with other character strings represented in UTF-8.

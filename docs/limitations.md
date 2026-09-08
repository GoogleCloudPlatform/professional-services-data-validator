# Data Validation Tool Restrictions and Limitations

## General

- Floating-point data types, e.g. Float and Double, are inexact by nature. Validations that involve conversion of floating-point data to string, e.g. `--hash` and `--concat`, can be problematic.
- Row validations using `--comparison-fields` require at least one comparison column that is not a primary key. When all table columns are used as primary keys, you must use `--concat` or `--hash` to validate the rows.
- Random row sampling uses IN lists to ensure the same data is pulled from the source and target systems. Higher random sample batch sizes increase the overhead on the query parser/planner for these systems. It is recommended to keep sample sizes below 10,000 rows.
- Random row sampling on composite primary keys adds additional challenges. For systems that DO NOT support tuple in lists (for example `(key, key2) IN ((val1, val2), (val3, val4))`) DVT falls back to an ORs-of-ANDs strategy which has higher resource requirements on query parsers/planners. It is recommended to keep sample sizes below 1,000 rows.

## BigQuery

- BigQuery does not have a 32 bit floating-point data type. Validations of systems containing 32 bit floats (e.g. Oracle BINARY_FLOAT) will likely be problematic when compared to FLOAT64.

## Db2 LUW

- Db2 LUW BLOB datatype is excluded from row validation due to incompatibility with concatenation/hashing functions.
- Db2 LUW XML data type is not compatible with the LENGTH function and is excluded from any column validations other than `--count`.
- Db2 LUW custom query validations require the creation of temporary views in the database to resolve additional data type information.
  This requires CREATE VIEW privileges for the connected user. Without it some data types will not be resolved correctly, for
  example CHARACTER will be interpreted as VARCHAR.

## Db2 z/OS

- Db2 z/OS XML data type is not compatible with aggregation functions or LENGTH function and is excluded from all column validations.
- Db2 z/OS custom query validations require the creation of temporary views in the database to resolve additional data type information.
  This requires CREATE VIEW privileges for the connected user. Without it some data types will not be resolved correctly, for
  example CHARACTER will be interpreted as VARCHAR.
- Db2 Z/OS ROWID data type is not supported.

## Oracle

- Requires the `oracledb` package to be installed as an extra dependency.
- Tables with lower case/mixed case identifiers are not currently supported. See [issue-1773](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1773) for details.
- Random row validation does not work with GUID/UUID primary keys. See [issue-1366](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1366) for details.
- Oracle LONG and LONG RAW data types are not supported.
- Oracle BLOB, CLOB, and NCLOB data types are not supported for row validation (except when used as part of a custom query).

## Snowflake

- Requires the `snowflake-connector-python` and `snowflake-sqlalchemy` packages to be installed as extra dependencies.

## SQL Server

- Requires the `pyodbc` package to be installed as an extra dependency plus an OS level ODBC driver manager and client.
- SQL Server does not have a function to "right trim" all whitespace, only spaces, therefore any validations relying on removal of trailing white space may encounter issues.
- The `text` and `ntext` data types are incompatible with the `len()` therefore the `datalength()` function ius used in it's place which will give different results for multibyte characters.
- The `image` data type is not currently supported, these columns are skipped when validated. See [issue-1578](https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1578) for details.
- SQL Server's `AVG()` function can overflow and return an arithmetic overflow error when calculating the average of a max-precision decimal column (e.g. `decimal(38)`). If you encounter this, exclude the column(s) from the list of columns passed to the `--avg` option.
- SQL Server has a limit of 2,100 query parameters. This effects `--use-random-row` validation when using composite primary keys. `--random-row-batch-size` should be limited to 2,100 minus margin for other filters, divided by the number of primary key columns.

## Sybase ASE

- Requires the `sqlalchemy_sybase` package to be installed as an extra dependency plus an OS level ODBC driver manager and client.
- Sybase ASE does not have a SHA-256 hash function, therefore row validations much use `--comparison-fields` or `--concat` only.
- Sybase ASE does not have a function to "right trim" all whitespace, only spaces, therefore any validations relying on removal of trailing white space may encounter issues.
- The `text` data type is not currently supported because many Sybase ASE SQL functions do not support the data type.
- Sybase ASE does not include window functions. At the current time this prevents Sybase ASE from being the source table in a `generate-table-partitions` command. A simple workaround is to flip the source and target connections.
- We have only tested against Sybase ASE. DVT Sybase support does not include other Sybase flavours.

## Teradata

- Requires the `teradatasql` package to be installed as an extra dependency.
- Teradata does not have a native SHA-256 hash function but a third party UDF can be [installed](https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/installation.md#teradata).

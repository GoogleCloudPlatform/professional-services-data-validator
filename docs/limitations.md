# Data Validation Tool Restrictions and Limitations

## BigQuery

- BigQuery does not have a 32 bit float data type. Validations of systems containing 32 bit floats (e.g. Oracle BINARY_FLOAT) will likely be problematic when compared to FLOAT64.

## Db2 LUW

- Db2 LUW BLOB datatype is excluded from row validation due to incompatibility with concatenation/hashing functions.

## Oracle

- Requires the `oracledb` package to be installed as an extra dependency.

## Snowflake

- Requires the `snowflake-connector-python` and `snowflake-sqlalchemy` packages to be installed as extra dependencies.

## SQL Server

- Requires the `pyodbc` package to be installed as an extra dependency plus an OS level ODBC driver manager and client.
- SQL Server does not have a function to "right trim" all whitespace, only spaces, therefore any validations relying on removal of trailing white space may encounter issues.
- The `text` and `ntext` data types are incompatible with the `len()` therefore the `datalength()` function ius used in it's place which will give different results for multibyte characters.
- The `image` data type is not currently supported, these columns are skipped when validated. See (issue-1578)[https://github.com/GoogleCloudPlatform/professional-services-data-validator/issues/1578] for details.

## Sybase ASE

- Requires the `sqlalchemy_sybase` package to be installed as an extra dependency plus an OS level ODBC driver manager and client.
- Sybase ASE does not have a SHA-256 hash function, therefore row validations much use `--comparison-fields` or `--concat` only.
- Sybase ASE does not have a function to "right trim" all whitespace, only spaces, therefore any validations relying on removal of trailing white space may encounter issues.
- The `text` data type is not currently supported because many Sybase ASE SQL functions do not support the data type.
- Sybase ASE does not include window functions. At the current time this prevents Sybase ASE from being the source table in a `generate-table-partitions` command. A simple workaround is to flip the source and target connections.
- We have only tested against Sybase ASE. DVT Sybase support does not include other Sybase flavours.

## Teradata

- Requires the `teradatasql` package to be installed as an extra dependency.
- Teradata does not have a native SHA-256 hash function but a third party UDF can be (installed)[https://github.com/GoogleCloudPlatform/professional-services-data-validator/blob/develop/docs/installation.md#teradata].
# Data Validation Tool Restrictions and Limitations

## BigQuery

- BigQuery does not have a 32 bit float data type. Validations of systems containing 32 bit floats (e.g. Oracle BINARY_FLOAT) will likely be problematic when compared to FLOAT64.

## Db2 LUW

- Db2 LUW XML data type is not compatible with the LENGTH function and is excluded from any column validations other than `--count`.

## Db2 z/OS

- Db2 z/OS XML data type is not compatible with aggregation functions or the LENGTH and is excluded from all column validations.

## Oracle

- Requires the `oracledb` package to be installed as an extra dependency.

## Snowflake

- Requires the `snowflake-connector-python` and `snowflake-sqlalchemy` packages to be installed as extra dependencies.

## SQL Server

- Requires the `pyodbc` package to be installed as an extra dependency plus an OS level ODBC driver manager and client.
- SQL Server does not have a function to "right trim" all whitespace, only spaces, therefore any validations relying on removal of trailing white space may encounter issues.
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
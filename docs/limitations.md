# Data Validation Tool Restrictions and Limitations


## SQL Server

- Requires the `pyodbc` package to be installed as an extra dependency.
- SQL Server does not have a function to "right trim" all whitespace, only spaces, therefore any validations relying on removal of trailing white space may encounter issues.
- The `image` data type is not currently supported, these columns are skipped when validated.

## Sybase

- Requires the `sqlalchemy_sybase` package to be installed as an extra dependency.
- Sybase ASE does not have a SHA-256 hash function, therefore row validations much use `--comparison-fields` or `--concat` only.
- Sybase ASE does not have a function to "right trim" all whitespace, only spaces, therefore any validations relying on removal of trailing white space may encounter issues.
- The `text` data type is not currently supported.

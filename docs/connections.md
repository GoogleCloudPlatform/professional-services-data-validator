# Data Validation Connections
You will need to create connections before running any validations with the data validation tool. The tool allows users to 
create these connections using the CLI. 

These connections will automatically be saved either to `~/.config/google-pso-data-validator/` or 
a directory specified by the env variable `PSO_DV_CONFIG_HOME`.

These commands can be used to create connections:

## Command template to create a connection:
```
data-validation connections add --connection-name my-conn-name source_type 
```

## Create a sample BigQuery connection:
```
data-validation connections add --connection-name MY-BQ-CONNECTION BigQuery --project-id MY-GCP-PROJECT
```

## Create a sample Teradata connection:
```
data-validation connections add --connection-name MY-TD-CONNECTION Teradata --host HOST_IP --port PORT --user_name USER_NAME --password PASSWORD
```

## List existing connections
```
data-validation connections list
```
## List supporting connection types
```
data-validation connections add -h
```
The data validation tool supports the following connection types.

* [Raw](#raw)
* [BigQuery](#google-bigquery)
* [Spanner](#google-spanner)
* [Teradata](#teradata)
* [Oracle](#oracle)
* [MSSQL](#mssql-server)
* [Snowflake](#snowflake)
* [Postgres](#postgres)
* [Redshift](#redshift)

As you see above, Teradata and BigQuery have different sets of custom arguments (for example project_id for BQ versus host for Teradata).

Every connection type requires its own configuration for connectivity. To find out the parameters for each connection type, use the following command.

```
data-validation connections add -c '<name>' <connection type> -h
```

Below is the expected configuration for each type.

## Raw
```
{
    # Raw JSON config for a connection
    "json": '{"source_type": "BigQuery", "project_id": "pso-kokoro-resources", "google_service_account_key_path": null}'
}
```

## Google BigQuery
```
{
    # Configuration Required for All Data Soures
    "source_type": "BigQuery",

    # BigQuery Specific Connection Config
    "project_id": "my-project-name",

    # (Optional) BigQuery JSON Config File for On-Prem usecases
    "google_service_account_key_path": "/path/to/key.json"
}
```

### User/Service account needs following BigQuery permissions to run this validator tool:
* bigquery.jobs.create (BigQuery JobUser role)
* bigquery.readsessions.create (BigQuery Read Session User)
* bigquery.tables.get (BigQuery Data Viewer)
* bigquery.tables.getData (BigQuery Data Viewer)

### If you plan to store validation results in BigQuery:
* bigquery.tables.update (BigQuery Data Editor)
* bigquery.tables.updateData (BigQuery Data Editor)

## Google Spanner
```
{
    # Configuration Required for All Data Soures
    "source_type": "Spanner",

    # GCP Project to use for Spanne
    "project_id": "my-project-name",
    
    # ID of Spanner instance to connect to
    "instance_id": "my-instance-id",

    # ID of Spanner database (schema) to connect to
    "database_id": "my-database-id",
                        
    # (Optional) Spanner JSON Config File for On-Prem usecases
    "google_service_account_key_path": "/path/to/key.json"
}
```

###  User/Service account needs following Spanner role to run this validator tool:
* roles/spanner.databaseReader

## Teradata
Please note the Teradata is not-native to this package and must be installed
via `pip install teradatasql` if you have a license.
```
{
    # Configuration Required for All Data Soures
    "source_type": "Teradata",

    # Connection Details
    "host": "127.0.0.1",
    "port":1025,
    "user_name":"my-user",
    "password":"my-password"
}
```

## Oracle
Please note the Oracle package is not installed by default. You will need to follow [cx_Oracle](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html) installation steps.
Then `pip install cx_Oracle`.
```
{
    # Configuration Required for All Data Soures
    "source_type": "Oracle",

    # Connection Details
    "host": "127.0.0.1",
    "port":1521,
    "user_name":"my-user",
    "password":"my-password",
    "database": "XE",

}
```

## MSSQL Server
Please note the MSSQL Server package is not installed by default. You will need to follow [SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server) installation steps.
Then `pip install pyodbc`.
```
{
    # Configuration Required for All Data Soures
    "source_type": "MSSQL",

    # Connection Details
    "host": "127.0.0.1",
    "port": 1433,
    "user": "my-user",
    "password": "my-password",
    "database": "my-db",

}
```

## Snowflake
```
{
    # Configuration Required for All Data Soures
    "source_type": "Snowflake",

    # Connection Details
    "user": "my-user",
    "password": "my-password",
    "account": "Snowflake account to connect to"
    "database":"my-db"
    "schema": "my-schema"
}
```

## Postgres
```
{
    # Configuration Required for All Data Soures
    "source_type": "Postgres",

    # Connection Details
    "host": "127.0.0.1",
    "port":5432,
    "user": "my-user",
    "password": "my-password",
    "database":"my-db"
}
```

## Redshift
```
{
    # Configuration Required for All Data Soures
    "source_type": "Redshift",

    # Connection Details
    "host": "127.0.0.1",
    "port":5439,
    "user": "my-user",
    "password": "my-password",
    "database":"my-db"
}
```


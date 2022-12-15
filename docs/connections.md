# Data Validation Connections
You will need to create connections before running any validations with the data validation tool. The tool allows users to 
create these connections using the CLI. 

These connections will automatically be saved either to `~/.config/google-pso-data-validator/` or 
a directory specified by the env variable `PSO_DV_CONFIG_HOME`.

## GCS Connection Management (recommended)

The connections can also be stored in GCS using `PSO_DV_CONFIG_HOME`.
To do so simply add the GCS path to the environment. Note that if this path is set, query validation configs will also be saved here.

eg.
`export PSO_DV_CONFIG_HOME=gs://my-bucket/my/connections/path/`

The following commands can be used to create connections:

## Command template to create a connection:
```
data-validation connections add --connection-name CONN_NAME source_type 
```

## Create a sample BigQuery connection:
```
data-validation connections add --connection-name MY_BQ_CONN BigQuery --project-id MY_GCP_PROJECT
```

## Create a sample Teradata connection:
```
data-validation connections add --connection-name MY_TD_CONN Teradata --host HOST_IP --port PORT --user-name USER-NAME --password PASSWORD
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
* [Postgres](#postgres)
* [MySQL](#mysql)
* [Redshift](#redshift)
* [FileSystem](#filesystem)
* [Impala](#Impala)
* [Hive](#Hive)
* [DB2](#DB2)
* [AlloyDB](#AlloyDB)

As you see above, Teradata and BigQuery have different sets of custom arguments (for example project_id for BQ versus host for Teradata).
Every connection type requires its own configuration for connectivity. To find out the parameters for each connection type, use the following command.

```
data-validation connections add -c CONN_NAME <connection type> -h
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
    # Configuration Required for All Data Sources
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
    # Configuration Required for All Data Sources
    "source_type": "Spanner",

    # GCP Project to use for Spanner
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
Please note that Teradata is not-native to this package and must be installed
via `pip install teradatasql` if you have a license.
```
{
    # Configuration Required for All Data Sources
    "source_type": "Teradata",

    # Connection Details
    "host": "127.0.0.1",
    "port":1025,
    "logmech":"TD2",
    "user_name":"my-user",
    "password":"my-password"
}
```

## Oracle
Please note the Oracle package is not installed by default. You will need to follow [cx_Oracle](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html) installation steps.
Then `pip install cx_Oracle`.
```
{
    # Configuration Required for All Data Sources
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
    # Configuration Required for All Data Sources
    "source_type": "MSSQL",

    # Connection Details
    "host": "127.0.0.1",
    "port": 1433,
    "user": "my-user",
    "password": "my-password",
    "database": "my-db",

}
```

## Postgres
```
{
    # Configuration Required for All Data Sources
    "source_type": "Postgres",

    # Connection Details
    "host": "127.0.0.1",
    "port":5432,
    "user": "my-user",
    "password": "my-password",
    "database":"my-db"
}
```

## AlloyDB
Please note AlloyDB supports same connection config as Postgres.
```
{
    # Configuration Required for All Data Sources
    "source_type": "Postgres",

    # Connection Details
    "host": "127.0.0.1",
    "port":5432,
    "user": "my-user",
    "password": "my-password",
    "database":"my-db"
}
```

## MySQL
```
{
    # Configuration Required for All Data Sources
    "source_type": "MySQL",

    # Connection Details
    "host": "127.0.0.1",
    "port":3306
    "user": "my-user",
    "password": "my-password",
    "database":"my-db"
}
```

## Redshift
```
{
    # Configuration Required for All Data Sources
    "source_type": "Redshift",

    # Connection Details
    "host": "127.0.0.1",
    "port":5439,
    "user": "my-user",
    "password": "my-password",
    "database":"my-db"
}
```

## FileSystem (CSV or JSON only)
```
{
    # Configuration Required for All Data Sources
    "source_type": "FileSystem",

    # Table name to use as a reference for file data
    "table_name": "my_table_name",
    
    # The local, s3, or GCS file path to the data
    "file_path": "gs://path/to/file",
    
    # The file type. Either 'csv' or 'json'
    "file_type":"csv"
}
```

## Impala
```
{
    # Configuration Required for All Data Sources
    "source_type": "Impala",

    # Connection Details
    "host": "127.0.0.1",
    "port": 10000,
    "database": "default",
    "auth_mechanism":"PLAIN"
}
```

## Hive
Please note that for Group By validations, the following property must be set in Hive:

`set hive:hive.groupby.orderby.position.alias=true`
 
 If you are running Hive on Dataproc, you will also need to install the following:
 ```
 pip install ibis-framework[impala]
 ```
 
```
{
    # Hive is based off Impala connector
    "source_type": "Impala",

    # Connection Details
    "host": "HIVE_IP_ADDRESS",
    "port": 10000,
    "database": "default",
    "auth_mechanism":"PLAIN"
}
```
Only Hive >=0.11 is supported due to [impyla](https://github.com/cloudera/impyla)'s dependency on HiveServer2.

## DB2
```
{
    # Configuration Required for All Data Sources
    "source_type": "DB2",

    # Connection Details
    "host": "localhost",
    "port": 50000,
    "driver": "ibm_db_sa",
    "user": "my-username",
    "password": "my-password",
    "database": "my-db",
    "url": "my-url",
}
```
# Data Validation Connections
You will need to create connections before running any validations with the data validation tool. The tool allows users to 
create these connections using the CLI. 

These connections will automatically be saved either to `~/.config/google-pso-data-validator/` or 
a directory specified by the env variable `PSO_DV_CONFIG_HOME`.

These commands can be used to create connections:

#### Command template to create a connection:
```
data-validation connections add --connection-name my-conn-name source_type 
```

#### Create a sample BigQuery connection:
```
data-validation connections add --connection-name MY-BQ-CONNECTION BigQuery --project-id MY-GCP-PROJECT
```


#### Create a sample Teradata connection:
```
data-validation connections add --connection-name MY-TD-CONNECTION Teradata --host HOST_IP --port PORT --user_name USER_NAME --password PASSWORD
```

#### List existing connections
````
data-validation connections list
````

As you see above, Teradata and BigQuery have different sets of custom arguments (for example project_id for BQ versus host for Teradata).  

Every source type requires its own configuration for connectivity.  Below is the expected configuration for each source type.

##### BigQuery
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

######  User/Service account needs following BigQuery permissions to run this validator tool:
* bigquery.jobs.create (BigQuery JobUser role)
* bigquery.readsessions.create (BigQuery Read Session User)
* bigquery.tables.get (BigQuery Data Viewer)
* bigquery.tables.getData (BigQuery Data Viewer)

######  If you plan to store validation results in BigQuery:
* bigquery.tables.update (BigQuery Data Editor)
* bigquery.tables.updateData (BigQuery Data Editor)


#####  Teradata
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

##### Oracle
Please note the Oracle package is not installed by default.  You will need to follow [cx_Oracle](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html) installation steps.
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

##### MSSQL Server
Please note the MSSQL Server package is not installed by default.  You will need to follow [SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server) installation steps.
Then `pip install pyodbc`.
```
{
    # Configuration Required for All Data Soures
    "source_type": "MSSQL",

    # Connection Details
    "host": "127.0.0.1",
    "port":1521,
    "user_name":"my-user",
    "password":"my-password",
    "database": "my-db",

}
```

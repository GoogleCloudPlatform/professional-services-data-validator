# Data Validation Connections
You will need to specify connection configuration (host, port, username, password etc.) to connect to the database before running any validations with the data validation tool. The tool validates connections when the configuration is provided via the `connections add` command before storing the connection in the filesystem. The configuration is stored as JSON key value pairs which are specific to the datasource type (BigQuery, Postgres, Oracle). DVT currently only stores the configuration information in the filesystem/GCS. Users can copy the contents of the file into a secret in the GCP secret manager. If the connection name is `mysql`, then the command
```
gcloud secrets create mysql --data-file="${HOME}/.config/google-pso-data-validator/mysql.connection.json"
```
copies the connection configuration to the secret manager with the name `mysql`.
Now DVT can read directly from the secret manager and the file `mysql.connection.json` can be deleted from the file system. DVT reads configuration information from the filesystem (by default) or from the secret manager if the secret manager type ("gcp") and project are provided. When DVT is run in a distributed environment (multiple VMs or containers) using a secret manager is recommended. Secret manager also keeps passwords and other configuration information secure and supports password rotation. The user or service account running data validation tool must have `roles/secretmanager.secretAccessor` either at the project level or at the individual secret level to access secrets stored in the secret manager. Please see (Secret Manger IAM roles)[https://cloud.google.com/secret-manager/docs/access-control]

By default the connection configuration is saved the directory `~/.config/google-pso-data-validator/`. If the env variable `PSO_DV_CONFIG_HOME` is specified the configuration information is stored here. `PSO_DV_CONFIG_HOME` can specify a location in the filesystem or GCS.  
eg.
`export PSO_DV_CONFIG_HOME=gs://my-bucket/my/connections/path/`.  If `PSO_DV_CONFIG_HOME` is specified, the query validation configs will also be saved there. 

If the secret-manager flags are present, the connection name should reference secret names. For example,
the following BigQuery connection references a connection with name 'bq' stored in project MY-PROJECT.

```
data-validation query \
    --secret-manager-type GCP \
    --secret-manager-project-id <MY-PROJECT> \
    --conn bq \
    --query 'select * from bigquery-public-data.new_york_citibike.citibike_trips limit 10;'

```
DVT only supports [Google Cloud Secret Manager](https://cloud.google.com/secret-manager) for storing and referencing secrets in your connection configuration.

## List existing connections
```
data-validation connections list
```
Lists connections that have been validated and stored in the filesystem. This command does not access secrets in the secret manager.

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
* [FileSystem](#filesystem-csv-or-json-only)
* [Impala](#Impala)
* [Hive](#Hive)
* [DB2](#DB2)
* [AlloyDB](#AlloyDB)
* [Snowflake](#snowflake)


Every connection type requires its own configuration for connectivity. To find out the parameters for each connection type, use the following command:

```
data-validation connections add -c CONN_NAME <connection type> -h
```

Below are the connection parameters for each database.

## Raw
```
data-validation connections add 
    --connection-name CONN_NAME Raw                     Connection name
    --json JSON                                         Raw JSON for connection
```
The raw JSON can also be found in the connection config file. For example,
`'{"source_type": "BigQuery", "project_id": "my-project-id"}'`

## Google BigQuery
```
data-validation connections add 
    --connection-name CONN_NAME BigQuery                Connection name
    --project-id MY_PROJECT                             Project ID where BQ data resides
    [--google-service-account-key-path PATH_TO_SA_KEY]  Path to SA key
```
DVT allows you to use and store service account keys. Google does *not* recommend using [service account keys](https://cloud.google.com/iam/docs/best-practices-service-accounts#service-account-keys). Google recommends using [service account impersonation](https://cloud.google.com/iam/docs/service-account-impersonation). 
### User/Service account needs following BigQuery permissions to run DVT:
* bigquery.jobs.create (BigQuery JobUser role)
* bigquery.readsessions.create (BigQuery Read Session User)
* bigquery.tables.get (BigQuery Data Viewer)
* bigquery.tables.getData (BigQuery Data Viewer)

### If you plan to store validation results in BigQuery:
* bigquery.tables.update (BigQuery Data Editor)
* bigquery.tables.updateData (BigQuery Data Editor)
The service account key option is not usable when the connection is stored in the secret manager. An example BigQuery configuration stored in the secret manager is `{"source_type": "BigQuery", "project_id": "xxx-flash-dance"}`

## Google Spanner
```
data-validation connections add 
    --connection-name CONN_NAME Spanner                 Connection name
    --project-id MY_PROJECT                             Project ID where BQ data resides
    --instance-id MY_INSTANCE                           Spanner instance to connect to
    --database-id MY-DB                                 Spanner database (schema) to connect to
    [--google-service-account-key-path PATH_TO_SA_KEY]  Path to SA key
```
Please see comments in the BigQuery section about using service account keys. An example Spanner configuration stored in the secret manager is `{"source_type": "xxxxxx", "project_id": "xxx-flash-dance"}`
###  User/Service account needs following Spanner role to run DVT:
* roles/spanner.databaseReader

## Teradata
Please note that Teradata is not-native to this package and must be installed
via `pip install teradatasql` if you have a license. An example Teradata configuration stored in the secret manager is `{"source_type": "Teradata", "host": "10.xx.xx.xx", "port": "1025", "user_name": "yyyy", "password": "zzzz"}`

```
data-validation connections add 
    --connection-name CONN_NAME Teradata                Connection name
    --host HOST                                         Teradata host
    --port PORT                                         Teradata port, defaults to 1025
    --user-name USER                                    Teradata user
    --password PASSWORD                                 Teradata password
    [--logmech LOGMECH]                                 Teradata logmech, defaults to "TD2"
```

## Oracle
Please note the Oracle package is not installed by default. You will need to follow [cx_Oracle](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html) installation steps.
Then `pip install cx_Oracle`. An example Oracle configuration stored in the secret manager is `{"source_type": "Oracle", "host": "10.xx.xx.xxx", "port": "1521", "user": "aaaaaaa", "password": "bbbbbbbb", "database": "XEPDB1"}`
```
data-validation connections add 
    --connection-name CONN_NAME Oracle                  Connection name
    --host HOST                                         Oracle host
    --port PORT                                         Oracle port, defaults to 1521
    --user USER                                         Oracle user
    --password PASSWORD                                 Oracle password
    --database DATABASE                                 Oracle database
```


### Oracle User permissions to run DVT:
* CREATE SESSION
* READ or SELECT on any tables to be validated
* Optional - Read on SYS.V_$TRANSACTION (required to get isolation level, if privilege is not given then will default to Read Committed, [more_details](https://docs.sqlalchemy.org/en/14/dialects/oracle.html#transaction-isolation-level-autocommit))

## MSSQL Server
MSSQL Server connections require [pyodbc](https://pypi.org/project/pyodbc/) as the driver: `pip install pyodbc`. For connection query parameter options, see https://docs.sqlalchemy.org/en/20/dialects/mssql.html#hostname-connections. An example MS SQL Server configuration stored in the secret manager is `{"source_type": "MSSQL", "host": "127.xx.xx.xx", "port": "1433", "user": "aaaaaaaaa", "password": "bbbbbbbbb", "database": "ccccccc"}`

```
data-validation connections add 
    --connection-name CONN_NAME MSSQL                   Connection name
    --host HOST                                         MSSQL host
    --port PORT                                         MSSQL port, defaults to 1433
    --user USER                                         MSSQL user
    --password PASSWORD                                 MSSQL password
    --database DATABASE                                 MSSQL database
```

## Postgres
An example Postgres configuration stored in the secret manager is `{"source_type": "Postgres", "host": "127.xx.xx.xxx", "port": "5432", "user": "aaaaaaaa", "password": "bbbbbbbb", "database": "cccccccccc"}`
```
data-validation connections add 
    --connection-name CONN_NAME Postgres                Connection name
    --host HOST                                         Postgres host
    --port PORT                                         Postgres port, defaults to 5432
    --user USER                                         Postgres user
    --password PASSWORD                                 Postgres password
    --database DATABASE                                 Postgres database
```

## AlloyDB
Please note AlloyDB supports same connection config as Postgres. An example AlloyDB configuration stored in the secret manager is `{"source_type": "Postgres", "host": "127.xx.xx.xxx", "port": "5432", "user": "aaaaaaaa", "password": "bbbbbbbb", "database": "cccccccccc"}`
```
data-validation connections add 
    --connection-name CONN_NAME Postgres                Connection name
    --host HOST                                         Postgres host
    --port PORT                                         Postgres port, defaults to 5432
    --user USER                                         Postgres user
    --password PASSWORD                                 Postgres password
    --database DATABASE                                 Postgres database
```

## MySQL
An example MySQL configuration stored in the secret manager is `{"source_type": "MySQL", "host": "127.xx.xx.xxx", "port": "3306", "user": "aaaaaaaa", "password": "bbbbbbb", "database": "ccccccccc"}`
```
data-validation connections add 
    --connection-name CONN_NAME MySQL                   Connection name
    --host HOST                                         MySQL host
    --port PORT                                         MySQL port, defaults to 3306
    --user USER                                         MySQL user
    --password PASSWORD                                 MySQL password
    --database DATABASE                                 MySQL database
```

## Redshift
An example Redshift configuration stored in the secret manager is `{"source_type": "Redshift", "host": "xxxx-cluster.xxxxx.xxxx.redshift.amazonaws.com", "port": "5439", "user": "aaaaaaa", "password": "bbbbbbb", "database": "cccccccc"}`
```
data-validation connections add 
    --connection-name CONN_NAME Redshift                Connection name
    --host HOST                                         Redshift host
    --port PORT                                         Redshift port, defaults to 5439
    --user USER                                         Redshift user
    --password PASSWORD                                 Redshift password
    --database DATABASE                                 Redshift database
```

## FileSystem (CSV or JSON only)
```
data-validation connections add 
    --connection-name CONN_NAME FileSystem              Connection name
    --table-name TABLE_NAME                             Table name to use as reference for file data
    --file-path FILE_PATH                               Local, GCS, or S3 file path
    --file-type FILE_TYPE                               File type (csv, json)
```

## Impala
An example Impala configuration stored in the secret manager is `{"source_type": "Impala", "host": "10.xxx.xx.xxx", "port": "10000", "database": "ccccccc"}`

```
data-validation connections add 
    --connection-name CONN_NAME Impala                  Connection name
    --host HOST                                         Impala host
    --port PORT                                         Impala port
    --database DATABASE                                 Impala database, defaults to "default"
    [--auth-mechanism AUTH_MECH]                        Auth mechanism, defaults to "PLAIN"
    [--user USER]                                       Impala user
    [--password PASSWORD]                               Impala password
    [--use-ssl USE_SSL]                                 Use SSL (True, False)
    [--timeout TIMEOUT]                                 Timeout, defaults to 45
    [--ca-cert CA_CERT]                                 CA Cert
    [--pool-size POOL_SIZE]                             Impala pool size, default to 8
    [--hdfs-client CLIENT]                              HDFS client
    [--use-http-transport TRANSPORT]                    HTTP Transport (True, False)
    [--http-path PATH]                                  HTTP Path
```

## Hive
Please note that for Group By validations, the following property must be set in Hive:

`set hive:hive.groupby.orderby.position.alias=true`
 
 If you are running Hive on Dataproc, you will also need to install the following:
 ```
 pip install ibis-framework[impala]
 ```
 
 Hive connections are based on the Ibis Impala connection which uses [impyla](https://github.com/cloudera/impyla).
 Only Hive >=0.11 is supported due to impyla's dependency on HiveServer2.

 ```
data-validation connections add 
    --connection-name CONN_NAME Impala                  Connection name
    --host HOST                                         Hive host
    --port PORT                                         Hive port, defaults to 10000
    --database DATABASE                                 Hive database, defaults to "default"
    [--auth-mechanism AUTH_MECH]                        Auth mechanism, defaults to "PLAIN"
    [--user USER]                                       Hive user
    [--password PASSWORD]                               Hive password
    [--use-ssl USE_SSL]                                 Use SSL (True, False)
    [--timeout TIMEOUT]                                 Timeout, defaults to 45
    [--ca-cert CA_CERT]                                 CA Cert
    [--pool-size POOL_SIZE]                             Hive pool size, default to 8
    [--hdfs-client CLIENT]                              HDFS client
    [--use-http-transport TRANSPORT]                    HTTP Transport (True, False)
    [--http-path PATH]                                  HTTP Path
```


## DB2
DB2 requires the `ibm_db_sa` package.
```
data-validation connections add 
    --connection-name CONN_NAME DB2                     Connection name
    --host HOST                                         DB2 host
    --port PORT                                         DB2 port, defaults to 50000
    --user USER                                         DB2 user
    --password PASSWORD                                 DB2 password
    --database DATABASE                                 DB2 database
    [--url URL]                                         URL link in DB2 to connect to
    [--driver DRIVER]                                   DB2 driver, defaults to "ibm_db_sa"
```

## Snowflake
Snowflake requires the `snowflake-sqlalchemy` and `snowflake-connector-python` packages.
For details on connection parameters, see the [Ibis Snowflake connection parameters](https://ibis-project.org/backends/snowflake/#connection-parameters). An example Snowflake configuration stored in the secret manager is `{"source_type": "Snowflake", "user": "aaaaaaaa", "password": "bbbbbbbb", "account": "ccccccccc", "database": "ddddddd/public"}`
```
data-validation connections add 
    --connection-name CONN_NAME Snowflake               Connection name
    --user USER                                         Snowflake user
    --password PASSWORD                                 Snowflake password
    --account ACCOUNT                                   Snowflake account 
    --database DATABASE/SCHEMA                          Snowflake database and schema, separated by a `/`
    [--connect-args CONNECT_ARGS]                       Additional connection args, default {}
```
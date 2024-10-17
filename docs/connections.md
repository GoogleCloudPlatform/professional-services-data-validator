# Data Validation Connections
You will need to create connections before running any validations with the data validation tool. The tool allows users to
create these connections using the CLI.

These connections will automatically be saved either to `~/.config/google-pso-data-validator/` or
a directory specified by the env variable `PSO_DV_CONN_HOME`.

## GCS Connection Management (recommended)

The connections can also be stored in GCS using `PSO_DV_CONN_HOME`.
To do so simply add the GCS path to the environment i.e
`export PSO_DV_CONN_HOME=gs://my-bucket/my/connections/path/`

## Using GCP Secret Manager
DVT supports [Google Cloud Secret Manager](https://cloud.google.com/secret-manager) for storing and referencing secrets in your connection configuration.

If the secret-manager flags are present, the remaining connection flags should reference secret names instead of the secret itself. For example,
the following BigQuery connection references a secret with name 'dvt-project-id' stored in project MY-PROJECT.

```
data-validation connections add \
    --secret-manager-type GCP \
    --secret-manager-project-id <MY-PROJECT> \
    --connection-name bq BigQuery \
    --project-id 'dvt-project-id'

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
* [FileSystem](#filesystem-csv-parquet-orc-json-only)
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
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
    --connection-name CONN_NAME Raw                     Connection name
    --json JSON                                         Raw JSON for connection
```
The raw JSON can also be found in the connection config file. For example,
`'{"source_type": "BigQuery", "project_id": "my-project-id"}'`

## Google BigQuery
```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
    --connection-name CONN_NAME BigQuery                Connection name
    --project-id MY_PROJECT                             Project ID where BQ data resides
    [--google-service-account-key-path PATH_TO_SA_KEY]  Path to SA key
```

### User/Service account needs following BigQuery permissions to run DVT:
* bigquery.jobs.create (BigQuery JobUser role)
* bigquery.readsessions.create (BigQuery Read Session User)
* bigquery.tables.get (BigQuery Data Viewer)
* bigquery.tables.getData (BigQuery Data Viewer)

### If you plan to store validation results in BigQuery:
* bigquery.tables.update (BigQuery Data Editor)
* bigquery.tables.updateData (BigQuery Data Editor)


## Google Spanner
```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
    --connection-name CONN_NAME Spanner                 Connection name
    --project-id MY_PROJECT                             Project ID where BQ data resides
    --instance-id MY_INSTANCE                           Spanner instance to connect to
    --database-id MY-DB                                 Spanner database (schema) to connect to
    [--google-service-account-key-path PATH_TO_SA_KEY]  Path to SA key
```

###  User/Service account needs following Spanner role to run DVT:
* roles/spanner.databaseReader

## Teradata
Please note that Teradata is not-native to this package and must be installed
via `pip install teradatasql` if you have a license.

```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
    --connection-name CONN_NAME Teradata                Connection name
    --host HOST                                         Teradata host
    --port PORT                                         Teradata port, defaults to 1025
    --user-name USER                                    Teradata user
    --password PASSWORD                                 Teradata password
    [--logmech LOGMECH]                                 Teradata logmech, defaults to "TD2"
    [--use-no-lock-tables USE_NO_LOCK_TABLES]           Use access lock for queries, defaults to "False"
    [--json-params JSON_PARAMS]                         Additional teradatasql JSON string parameters (Optional)
```

## Oracle
Please note the Oracle package is not installed by default. You will need to follow [cx_Oracle](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html) installation steps.
Then `pip install cx_Oracle`.
```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
    --connection-name CONN_NAME Oracle                  Connection name
    --host HOST                                         Oracle host
    --port PORT                                         Oracle port, defaults to 1521
    --user USER                                         Oracle user
    --password PASSWORD                                 Oracle password
    --database DATABASE                                 Oracle database
    [--url URL]                                         SQLAlchemy connection URL
```

### Oracle User permissions to run DVT:
* CREATE SESSION
* READ or SELECT on any tables to be validated
* Optional - Read on SYS.V_$TRANSACTION (required to get isolation level, if privilege is not given then will default to Read Committed, [more_details](https://docs.sqlalchemy.org/en/14/dialects/oracle.html#transaction-isolation-level-autocommit))

### Using an Oracle wallet:

After creating an Oracle wallet and supporting configuration you can add the connection using the `--url` option, remembering to set `TNS_ADMIN` correctly before doing so. For example:

```
export TNS_ADMIN=/opt/dvt/dvt_tns_admin

data-validation connections add \
 --connection-name ora_secure Oracle \
 --url="oracle+cx_oracle://@dvt_prod_db"
```

## MSSQL Server
MSSQL Server connections require [pyodbc](https://pypi.org/project/pyodbc/) as the driver: `pip install pyodbc`.
For connection query parameter options, see https://docs.sqlalchemy.org/en/20/dialects/mssql.html#hostname-connections.

```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
    --connection-name CONN_NAME MSSQL                   Connection name
    --host HOST                                         MSSQL host
    --port PORT                                         MSSQL port, defaults to 1433
    --user USER                                         MSSQL user
    --password PASSWORD                                 MSSQL password
    --database DATABASE                                 MSSQL database
    [--url URL]                                         SQLAlchemy connection URL
    [--query QUERY]                                     Connection query parameters i.e. '{"TrustServerCertificate": "yes"}'
```

## Postgres
```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
    --connection-name CONN_NAME Postgres                Connection name
    --host HOST                                         Postgres host
    --port PORT                                         Postgres port, defaults to 5432
    --user USER                                         Postgres user
    --password PASSWORD                                 Postgres password
    --database DATABASE                                 Postgres database
```
DVT uses psycopg2, a Python PostgreSQL adapter which supports a large number of connection parameters including those to connect via TLS, [the complete list is here](https://www.postgresql.org/docs/current/libpq-envars.html). The parameters provided to DVT via the `connections add` command take precedence over the environment variables.

### Example TLS connection
```
export PGSSLCERT="/path/to/certs/client-cert.pem" \
export PGSSLKEY=/path/to/certs/client-key.pem \
export PGSSLROOTCERT=/path/to/certs/server-ca.pem \
export PGSSLMODE=verify-ca
data-validation connections add --connection-name pg_tls_ca Postgres \
--host=10.1.0.2 --user=dvt_user --password=secret-password-123 \
--database=appdb
```

## AlloyDB
Please note AlloyDB supports same connection config as Postgres.
```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
    --connection-name CONN_NAME Postgres                Connection name
    --host HOST                                         Postgres host
    --port PORT                                         Postgres port, defaults to 5432
    --user USER                                         Postgres user
    --password PASSWORD                                 Postgres password
    --database DATABASE                                 Postgres database
```

## MySQL
```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
    --connection-name CONN_NAME MySQL                   Connection name
    --host HOST                                         MySQL host
    --port PORT                                         MySQL port, defaults to 3306
    --user USER                                         MySQL user
    --password PASSWORD                                 MySQL password
    --database DATABASE                                 MySQL database
```

## Redshift
```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
    --connection-name CONN_NAME Redshift                Connection name
    --host HOST                                         Redshift host
    --port PORT                                         Redshift port, defaults to 5439
    --user USER                                         Redshift user
    --password PASSWORD                                 Redshift password
    --database DATABASE                                 Redshift database
```

## FileSystem (CSV, ORC, PARQUET or JSON only)
```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
    --connection-name CONN_NAME FileSystem              Connection name
    --table-name TABLE_NAME                             Table name to use as reference for file data
    --file-path FILE_PATH                               Local, GCS, or S3 file path
    --file-type FILE_TYPE                               File type (csv, json)
```

## Impala
```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
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
 Only Hive >=0.11 is supported due to [impyla](https://github.com/cloudera/impyla)'s dependency on HiveServer2.

 Hive connections are based on the Ibis Impala connection which uses [impyla](https://github.com/cloudera/impyla).
 Only Hive >=0.11 is supported due to impyla's dependency on HiveServer2.

 ```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
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
DB2 requires the `ibm_db_sa` package. We currently support only IBM DB2 LUW - Universal Database for Linux/Unix/Windows versions 9.7 onwards.
```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
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
For details on connection parameters, see the [Ibis Snowflake connection parameters](https://ibis-project.org/backends/snowflake/#connection-parameters).
```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
    --connection-name CONN_NAME Snowflake               Connection name
    --user USER                                         Snowflake user
    --password PASSWORD                                 Snowflake password
    --account ACCOUNT                                   Snowflake account
    --database DATABASE/SCHEMA                          Snowflake database and schema, separated by a `/`
    [--connect-args CONNECT_ARGS]                       Additional connection args, default {}
```

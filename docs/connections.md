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

If the secret-manager flags are present, any of the remaining connection flags can reference secret names instead of the secret itself.

Example 1: A BigQuery connection referencing a secret with name PROJECT_SECRET stored in project PROJECT_NAME:

```sh
data-validation connections add \
    --secret-manager-type GCP \
    --secret-manager-project-id PROJECT_NAME \
    --connection-name bq BigQuery \
    --project-id PROJECT_SECRET
```

Example 2: A PostgreSQL connection referencing a mixture of secrets (for `--host` and `--password`) stored in project PROJECT_NAME and simple string tokens:

```sh
data-validation connections add \
    --secret-manager-type GCP \
    --secret-manager-project-id PROJECT_NAME \
    --connection-name CONN Postgres \
    --host=HOST_SECRET \
    --user=USER_NAME \
    --password=PASSWORD_SECRET \
    --database=DATABASE
```

Example 3: An entire Oracle Credential alias stored as a secret with name CREDENTIAL_SECRET stored in project PROJECT_NAME:

```sh
data-validation connections add \
    --secret-manager-type GCP \
    --secret-manager-project-id PROJECT_NAME \
    --connection-name ora_uat Oracle \
    --connect-args=CREDENTIAL_SECRET
```

## List existing connections

```sh
data-validation connections list
```

## Delete an existing connection

```sh
data-validation connections delete -c CONN_NAME
```

## Describe an existing connection

```sh
data-validation connections describe -c CONN_NAME
```

## List supported connection types

```sh
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
* [FileSystem](#filesystem-csv-orc-parquet-or-json-only)
* [Impala](#impala)
* [Hive](#hive)
* [DB2](#db2)
* [AlloyDB](#alloydb)
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
    [--api-endpoint API_ENDPOINT]                       BigQuery API endpoint (e.g.
                                                        "https://bigquery-mypsc.p.googleapis.com)
    [--storage-api-endpoint STORAGE_API_ENDPOINT]       BigQuery Storage API endpoint (e.g.
                                                        "bigquerystorage-mypsc.p.googleapis.com)
                                                        Note this is a GRPC endpoint and does not
                                                        include a URI scheme.
```

### User/Service account needs following BigQuery permissions to run DVT

* bigquery.jobs.create (BigQuery JobUser role)
* bigquery.readsessions.create (BigQuery Read Session User)
* bigquery.tables.get (BigQuery Data Viewer)
* bigquery.tables.getData (BigQuery Data Viewer)

### If you plan to store validation results in BigQuery

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
    [--api-endpoint API_ENDPOINT]                       Spanner API endpoint (e.g.
                                                        "https://spanner-mypsc.p.googleapis.com")
```

### User/Service account needs following Spanner role to run DVT

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
    [--json-params JSON_PARAMS]                         Additional teradatasql JSON string dict (Optional)
```

## Oracle

Please note the Oracle package is not installed by default. You will need to follow [python-oracledb](https://python-oracledb.readthedocs.io/en/latest/user_guide/installation.html) installation steps.
Then `pip install oracledb`. DVT uses oracledb in Thin Mode by default which permits TLS and mTLS connections. You can also enable thick mode by specifying the `--thick-mode` parameter. Thick mode requires installation of Oracle client libraries. Differences between Thin and Thick mode are [discussed in the docs](https://python-oracledb.readthedocs.io/en/latest/user_guide/appendix_b.html#)

```
data-validation connections add
    [--secret-manager-type <None|GCP>]                  Secret Manager type (None, GCP)
    [--secret-manager-project-id SECRET_PROJECT_ID]     Secret Manager project ID
    --connection-name CONN_NAME Oracle                  Connection name
    [--thick-mode]                                      Use Thick mode, requires local Oracle client libraries
    [--host HOST]                                       Oracle host
    [--port PORT]                                       Oracle port, defaults to 1521
    [--user USER]                                       Oracle user, if not specified using credentials stored in wallet (see below)
    [--protocol PROTOCOL]                               Oracle networking protocol (TPC, TPCS)
    [--password PASSWORD]                               Oracle password
    [--database DATABASE]                               Oracle database
    [--connect-args CONNECT_PARAMS]                     Additional oracledb ConnectParams, JSON String dict
    [--url URL]                                         SQLAlchemy connection URL
```

### --url note

Note that the `--url` option allows specification of a SQLAlchemy URL, not an Oracle Easy Connect URL. This can be used to supply oracledb parameters inline, for example:
```
"oracle+oracledb://dvt_user:dvt_user@localhost:1521/?service_name=pdb1&disable_oob=true"
```
or in conjunction with `--connect-args` to combine a SQLAlchemy URL and additional oracledb parameters, for example:
```
{"source_type": "Oracle", "url": "oracle+oracledb://dvt_user:dvt_user@localhost:1521/?service_name=pdb1", "connect_args": "{\"disable_oob\": true}"}
```
See [SQLAlchemy documentation](https://docs.sqlalchemy.org/en/20/dialects/oracle.html#module-sqlalchemy.dialects.oracle.oracledb) for additional details. You may also use `--thick-mode` with the `--url` option.

### Oracle User permissions to run DVT

* CREATE SESSION
* READ or SELECT on any tables to be validated
* Optional - Read on SYS.V_$TRANSACTION (required to get isolation level, if privilege is not given then will default to Read Committed, [more_details](https://docs.sqlalchemy.org/en/14/dialects/oracle.html#transaction-isolation-level-autocommit))

### Additional Connect parameters, using TLS, mTLS connections and running DVT within a container
oracledb supports a large number of connection parameters documented as [ConnectParams](https://python-oracledb.readthedocs.io/en/latest/api_manual/connect_params.html#ConnectParams.set). Any of these params can be set by providing the `--connect-args` as a python dict.

For setting up a TLS connection, specify the configuration directory where `tnsnames.ora` is located, the wallet directory where `ewallet.pem` is located and the distinguished name of the server used when creating the certificate. The protocol, host, port and service_name are best specified in `tnsnames.ora` as they take precedence. For example, the `--connect-args` parameter can be specified as follows:

```
data-validation connections add \
 --connection-name ora_secure Oracle --user USER --password PASSWORD \
 --connect-args='{ "wallet_password": PASSWORD, "wallet_location": WALLET_DIR, "config_dir": CONFIG_DIR, "ssl_server_cert_dn": DISTINGUISHED_NAME}'
```
When DVT is running in a container, you may need to specify  `"disable_oob": True,` as one of the key value pairs in the `connect-args` dictionary to connect to Oracle.

### Using credentials from a wallet
When a user name is not specified, credentials (user name and password) are assumed to be in a wallet. Thick mode is automatically used, so Oracle client libraries are required. Only [the name of the credential created with the `mkstore createCredential` command](https://docs.oracle.com/en/database/oracle/oracle-database/23/dbseg/using-the-orapki-utility-to-manage-pki-elements.html#GUID-25509071-ABC0-4A0E-A3DB-4D4F61024F25), the `dsn`, is required. `config_dir` indicating location of `tnsnames.ora` and `sqlnet.ora` if not provided, is assumed from the environment variable `TNS_ADMIN`. Other connection parameters must be specified in `tnsnames.ora`. For example, the following is sufficient
```
data-validation connections add \
 --connection-name ora_secure Oracle \
 --connect-args='{"dsn": TNS_ALIAS}'
```

## MSSQL Server

MSSQL Server connections require [pyodbc](https://pypi.org/project/pyodbc/) as the driver: `pip install pyodbc`.
For connection query parameter options, see <https://docs.sqlalchemy.org/en/20/dialects/mssql.html#hostname-connections>.

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

```sh
export PGSSLCERT="/path/to/certs/client-cert.pem" \
export PGSSLKEY=/path/to/certs/client-key.pem \
export PGSSLROOTCERT=/path/to/certs/server-ca.pem \
export PGSSLMODE=verify-ca
data-validation connections add --connection-name CONN_NAME Postgres \
--host=HOST_NAME --user=USER --password=PASSWORD \
--database=DATABASE
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
    --file-type FILE_TYPE                               File type (csv, json, orc, parquet)
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

 ```sh
 pip install ibis-framework[impala]
 ```

 Hive connections are based on the Ibis Impala connection which uses [impyla](https://github.com/cloudera/impyla). Only Hive >= 0.11 is supported due to impyla's dependency on HiveServer2.

 When Kerberos needs to be used, it is necessary to set `--auth-mechanism` to `GSSAPI`.

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
    [--kerberos-service-name KERBEROS_SERVICE_NAME]     Desired Kerberos service name ('impala' if not provided)
    [--use-ssl USE_SSL]                                 If connecting to HiveServer2, defaults to False
    [--timeout TIMEOUT]                                 Timeout, defaults to 45
    [--ca-cert CA_CERT]                                 Local path to 3rd party CA certificate
    [--pool-size POOL_SIZE]                             Hive pool size, default to 8
    [--hdfs-client CLIENT]                              An existing HDFS client
    [--use-http-transport TRANSPORT]                    If HTTP proxy is provided, defaults to False
    [--http-path PATH]                                  URL path of HTTP proxy

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
    [--url URL]                                         SQLAlchemy connection URL
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
    [--connect-args CONNECT_ARGS]                       Additional connection args, JSON String dict, default {}
```

To connect to Snowflake using key-pair authentication you will need to use the `--connect-args` options. Example content from a connection file is included below for reference:
```
{"source_type": "Snowflake", "user": USER_NAME, "password": "", "account": ACCOUNT, "database": DATABASE, "connect_args": '{"private_key_file": PATH_TO_RSA_KEY/RSA_KEY.p8, "private_key_file_pwd": PASSPHRASE}'}
```

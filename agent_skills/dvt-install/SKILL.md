---
name: DVT Installation Skill
description: Instructions and commands for installing the Data Validation Tool (DVT) and its connection-specific dependencies.
---

# DVT Installation Skill

This skill provides instructions and commands for installing the Data Validation Tool (DVT) and its connection-specific dependencies.

## Local Installation Workflow

To install DVT in a local virtual environment, first verify your environment:

### Check Python Version 3.9.0-3.12.1
```bash
python3 --version
```
Ensure it is 3.9.0-3.12.1 (no higher/lower versions supported).

### Check if DVT is Installed
```bash
data-validation -h
```
Or via Python module:
```bash
python3 -m data_validation -h
```
If data_validation is already installed then ask the user if they want to upgrade it.

### Proceed with Installation

1.  **Create a Virtual Environment:**
    ```bash
    python3 -m venv .venv
    ```
2.  **Upgrade Pip:**
    ```bash
    .venv/bin/pip install --upgrade pip
    ```
4.  **Install DVT:**
    ```bash
    .venv/bin/pip install google-pso-data-validator
    ```
    *Note: For local development installation from source, use `.venv/bin/pip install .[dev]` from the repository root directory.*

---

## Containerized Installation Workflow

To build a Docker image with DVT, refer to the provided scripts and samples in the repository:
- Main build script: `./build_docker.sh`
- Sample Dockerfiles with specific driver prerequisites:
  - `samples/docker/Dockerfile_sql_server_alpine`
  - `samples/docker/Dockerfile_sql_server_debian`

---

## Database-Specific Setup

The following databases require additional packages or drivers. Natively supported databases like BigQuery and PostgreSQL are omitted here unless they require special UDFs.

### Teradata
- Requires `teradatasql` package.
- Requires a UDF for sha256 hashing on the Teradata instance.
- See [references/teradata_udf.md](./references/teradata_udf.md) for UDF details.
```bash
source venv/bin/activate && pip install teradatasql
```

### Oracle
- Requires `oracledb` package.
- Thin mode is default. Thick mode requires Oracle client libraries.
- **IMPORTANT NOTE:** The Oracle thick client should only be required when using an Oracle wallet for **authentication** (credentials stored in the wallet). A wallet used only for **TLS** (encryption) should be fine with the thin client included with `oracledb`.
- See [references/oracle_client.md](./references/oracle_client.md) for thick client details.
```bash
source venv/bin/activate && pip install oracledb
```

### SQL Server
- Requires `pyodbc` package and ODBC drivers.
- **NOTE:** `pyodbc` requires `gcc` to be installed on the system.
- See [references/sql_server_odbc.md](./references/sql_server_odbc.md) for driver details.
```bash
source venv/bin/activate && pip install pyodbc
```

### Hive & Impala
- Requires `ibis-framework[impala]` package.
```bash
source venv/bin/activate && pip install ibis-framework[impala]
```

### Db2 (LUW or z/OS)
- Requires `ibm_db_sa` package.
```bash
source venv/bin/activate && pip install ibm_db_sa
```

### Snowflake
- Requires `snowflake-sqlalchemy` and `snowflake-connector-python`.
```bash
source venv/bin/activate && pip install snowflake-sqlalchemy snowflake-connector-python
```

### Sybase ASE
- Requires `sqlalchemy_sybase` package.
```bash
source venv/bin/activate && pip install sqlalchemy_sybase
```

---

## Result Handler Setup

To store validation results, you need to set up a result handler.
- See [references/result_handler_setup.md](./references/result_handler_setup.md) for steps to create results tables in BigQuery or PostgreSQL.

---

## Permissions & Configuration Best Practices

### Cloud Roles (IAM)
Before concluding installation is successful for GCP data sources, verify the following IAM roles are granted:
- **BigQuery:** `BigQuery JobUser`, `BigQuery Read Session User`, and `BigQuery Data Viewer` (on the tables being validated).

### Secret Manager
For secure installations, DVT supports GCP Secret Manager for connection strings. Use this as a best practice path instead of hardcoding credentials in connection files.

### Environment Variables
- **`PSO_DV_CONN_HOME`**: Use this variable to specify where connection files are stored.
- **GCS Centralized Management:** Point it to a GCS bucket (e.g., `export PSO_DV_CONN_HOME=gs://my-bucket/connections/`) for centralized management, especially in containerized or distributed environments.

---

## Common Issues

### Missing System Headers
- If `pip install pyodbc` or `pip install psycopg2` fails, it is almost always due to missing `gcc` or `python3-dev`. Ensure they are installed on the host system.

### Authentication Failures
- If BigQuery or Spanner connections fail with authentication errors, verify that application default credentials are set:
  ```bash
  gcloud auth application-default login
  ```

---

## Verification

To prove the installation worked and DVT is ready to use, run the following command:
```bash
source venv/bin/activate && data-validation --version
```
This confirms the tool is accessible and the virtual environment is correctly configured.

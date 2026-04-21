# Plan: DVT Installation Agent Skill

This document outlines the plan for creating an agent skill (`SKILL.md`) to assist with the installation of the Data Validation Tool (DVT).

## Objective

Create a comprehensive `SKILL.md` file that guides an agent through the process of installing DVT and its optional dependencies, stored in `agent_skills/dvt-install/SKILL.md`.

## Scope Constraints

- **Location:** The skill file will be created in `agent_skills/dvt-install/SKILL.md`.
- **Database Coverage:** The skill will **only** explicitly discuss connection types that have extra requirements (e.g., additional Python packages or system drivers).
- **References:** Complex OS-level driver installations and external dependencies (Oracle Client, SQL Server ODBC, Teradata UDF) will be documented in a separate `references` directory.
- **Result Handlers:** The skill will cover setup of BigQuery and PostgreSQL result handlers.
- **Permissions & Auth:** The skill will cover IAM roles, Secret Manager best practices, and `PSO_DV_CONN_HOME` configuration.

## Proposed Directory Structure

```
agent_skills/dvt-install/
├── SKILL.md
└── references/
    ├── oracle_client.md
    ├── sql_server_odbc.md
    ├── teradata_udf.md
    └── result_handler_setup.md
```

## Information Gathered

### 1. Core Installation
- Standard: `pip install google-pso-data-validator`
- Local Dev: `pip install .`

### 2. Connection-Specific Dependencies (With Extra Requirements)
- **Teradata:** Requires `pip install teradatasql` and a UDF for sha256 hashing.
- **Oracle:** Requires `pip install oracledb`. Thin mode is default, Thick mode requires Oracle client libraries.
- **SQL Server:** Requires `pip install pyodbc` and ODBC drivers. Note: `pyodbc` requires `gcc` to be installed on the system.
- **Hive & Impala:** Requires `pip install ibis-framework[impala]`
- **Db2:** Requires `ibm_db_sa`
- **Snowflake:** Requires `snowflake-sqlalchemy`, `snowflake-connector-python`
- **Sybase:** Requires `sqlalchemy_sybase`

## Proposed Content

### SKILL.md Structure

1.  **Local Installation Workflow:** Includes environment verification (Python version, DVT check) and core installation steps.
2.  **Containerized Installation Workflow:** References to build scripts.
3.  **Database-Specific Setup:** Links to reference files for complex drivers/UDFs and simple `pip` commands for others.
4.  **Result Handler Setup:** Link to reference file for BigQuery and PostgreSQL setup.
5.  **Permissions & Configuration Best Practices:**
    - **IAM Roles:** Verification of BigQuery JobUser, Read Session User, and Data Viewer roles.
    - **Secret Manager:** Using GCP Secret Manager for secure installations.
    - **Environment Variables:** Usage of `PSO_DV_CONN_HOME` (this is required for containerized installations to enable GCS paths for connection files).
    - **Least Privilege:** Best practices for using read-only users for source/target data sources.
6.  **Common Issues:** Troubleshooting missing headers and auth failures.
7.  **Verification:** Commands to prove the installation worked (e.g., `data-validation --version`).

### references/oracle_client.md
- Instructions for installing Oracle Instant Client (relevant for Thick mode).
- **Recommendation:** Use Oracle Instant Client for installations requiring Thick mode.
- **NOTE:** The Oracle thick client should only be required when using an Oracle wallet for **authentication** (credentials stored in the wallet). A wallet used only for **TLS** (encryption) should be fine with the thin client included with `oracledb`.

### references/sql_server_odbc.md
- Instructions for installing ODBC drivers.

### references/teradata_udf.md
- Information on installing the UDF for sha256 hashing on Teradata.

### references/result_handler_setup.md
- Steps for creating results dataset/table in BigQuery (Terraform or gcloud).
- SQL snippets for creating schema and user permissions in PostgreSQL.

### Common Issues
- **Missing System Headers:** Reminder that if `pip install pyodbc` or `psycopg2` fails, it is likely missing `gcc` or `python3-dev`.
- **Auth Failures:** Reminder to check `gcloud auth application-default login` if BigQuery/Spanner connections fail.

## References

- [installation.md](file:///usr/local/google/home/neiljohnson/github/professional-services-data-validator2/docs/installation.md)
- [connections.md](file:///usr/local/google/home/neiljohnson/github/professional-services-data-validator2/docs/connections.md)
- [docker/README.md](file:///usr/local/google/home/neiljohnson/github/professional-services-data-validator2/samples/docker/README.md)

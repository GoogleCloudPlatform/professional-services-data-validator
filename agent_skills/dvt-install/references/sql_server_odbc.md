# SQL Server ODBC Driver Reference

This document provides information on SQL Server ODBC driver requirements for the Data Validation Tool (DVT).

## Pyodbc Prerequisite

The Python package `pyodbc` is used by DVT to connect to SQL Server.

-   **System Headers Required:** `pyodbc` compiles C extensions during installation. Therefore, it requires `gcc` and Python development headers (`python3-dev`) to be installed on the host system.

### Installation of Prerequisites (Debian/Ubuntu)
```bash
sudo apt-get update && sudo apt-get install -y gcc python3-dev
```

## ODBC Driver Installation

In addition to `pyodbc`, you must install the Microsoft ODBC driver for SQL Server on the operating system.

### Example: Installing ODBC Driver 18 on Debian

Refer to the official Microsoft documentation for the latest instructions. A typical installation pattern in a Dockerfile looks like this:

```dockerfile
# Install prerequisites
RUN apt-get update && apt-get install -y gnupg2 curl

# Add Microsoft package repository
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Install the driver
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

*Note: Adjust the repository URL (`prod.list`) based on your specific Linux distribution and version.*

## Connection Configuration

When adding a SQL Server connection in DVT, you may need to specify the driver name in the connection query parameters if it's not the default.

Example:
```bash
data-validation connections add --connection-name my_sql_server MSSQL \
  --host=HOST --database=DB --user=USER --password=PWD \
  --query='{"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes"}'
```

# Oracle Client Installation Reference

This document provides information on Oracle client requirements for the Data Validation Tool (DVT).

## Thin vs Thick Mode

DVT uses the `oracledb` Python package to connect to Oracle databases. This package can run in two modes:

1.  **Thin Mode (Default):**
    - Does not require Oracle client libraries.
    - Supports standard TLS and mTLS connections.
    - Recommended for most installations, especially in containers.

2.  **Thick Mode:**
    - Requires installation of Oracle client libraries (Instant Client).
    - Required for specific features like advanced security options or native network encryption that are not supported in Thin mode.

## Oracle Wallet Usage Constraints

The choice between Thin and Thick mode often depends on how you use Oracle Wallets:

-   **Wallet for Authentication:** If you are using an Oracle wallet to store **credentials** (user name and password) without specifying them in the connection command, you **MUST** use **Thick mode**. This requires installing the Oracle Instant Client.
-   **Wallet for TLS:** If you are using a wallet solely for **TLS encryption** (securing the communication channel) and providing credentials explicitly or via environment variables, **Thin mode is sufficient**. You do not need to install the Oracle Instant Client.

## Installing Oracle Instant Client (Thick Mode)

**Recommendation:** Our recommendation is to use the Oracle Instant Client for installations requiring Thick mode.

If you determine that Thick mode is required:

1.  Download the Oracle Instant Client `.rpm` or `.zip` files for your operating system from the Oracle website.
2.  Install the libraries. For example, in a Debian-based Dockerfile:
    ```dockerfile
    # Example snippet
    COPY oracle/oracle-instantclient*.rpm /tmp/
    RUN apt-get update && apt-get install -y alien
    RUN alien -i /tmp/oracle-instantclient*.rpm
    ```
3.  Set the `LD_LIBRARY_PATH` environment variable to point to the installed libraries.
4.  In DVT, enable thick mode by adding the `--thick-mode` flag when adding the connection or specifying it in the connection configuration.

Refer to the official [python-oracledb documentation](https://python-oracledb.readthedocs.io/en/latest/user_guide/installation.html) for detailed installation steps for your specific OS.

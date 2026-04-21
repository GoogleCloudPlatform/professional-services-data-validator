# Teradata UDF Reference

This document provides information on the Teradata User Defined Function (UDF) required for specific validations in the Data Validation Tool (DVT).

## SHA256 UDF Requirement

If you plan to perform **row-level hashing** validations on Teradata, DVT requires a UDF that implements the `sha256` algorithm on your Teradata instance.

Teradata does not always provide a native `sha256` function in older versions or specific configurations, so a UDF is often necessary to ensure DVT can compute hash values for comparison.

## Installation

1.  **Download or Source the UDF:**
    - You can find examples and community implementations for Teradata SHA256 UDFs. A common reference is mentioned in the DVT installation guide pointing to Teradata community downloads.
    - Example link from docs: [SHA-2 UDFs for Teradata](https://downloads.teradata.com/forum/extensibility/sha-2-udfs-for-teradata)

2.  **Install on Teradata Instance:**
    - Follow the instructions provided with the UDF to compile and install it on your Teradata database. This typically involves running SQL commands to create the function with the appropriate C source code or object files.
    - You will need database administrator privileges on the Teradata instance to install UDFs.

Once installed, DVT will be able to use this function during row validation runs involving Teradata tables.

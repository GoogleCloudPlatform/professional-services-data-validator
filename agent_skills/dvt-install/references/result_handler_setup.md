# Result Handler Setup Reference

This document provides instructions for setting up result handlers to store validation results from the Data Validation Tool (DVT).

---

## BigQuery Result Handler

By default, DVT writes results to `stdout`. To store results in BigQuery, you must create a specific dataset and table.

### Method 1: Cloud SDK (gcloud)

1.  **Create Dataset:**
    ```bash
    bq mk pso_data_validator
    ```

2.  **Create Table:**
    Create a table with time partitioning and clustering for performance. You will need the schema file from the DVT repository (`terraform/results_schema.json`).
    ```bash
    bq mk --table \
      --time_partitioning_field start_time \
      --clustering_fields validation_name,run_id \
      pso_data_validator.results \
      terraform/results_schema.json
    ```

### Method 2: Terraform

If you prefer Infrastructure as Code, you can use the provided Terraform configurations in the repository.

1.  Navigate to the `terraform` directory:
    ```bash
    cd terraform
    ```
2.  Update `variables.tf` with your project ID.
3.  Run Terraform commands:
    ```bash
    terraform init
    terraform apply
    ```

---

## PostgreSQL Result Handler

To store results in a PostgreSQL database, you must create a connection configuration with a user that has privileges to write to a results table.

### Database Setup

Run the following SQL commands on your PostgreSQL instance to create a schema and user:

```sql
-- Create a user for DVT results
CREATE USER dvt_results_writer WITH PASSWORD 'S3cr3t!';

-- Create a schema for results
CREATE SCHEMA pso_data_validator;

-- Grant necessary permissions
GRANT CREATE ON SCHEMA pso_data_validator TO dvt_results_writer;
GRANT USAGE ON SCHEMA pso_data_validator TO dvt_results_writer;
```

The table will be automatically created by DVT when the first validation result is written, provided the user has `CREATE` privileges on the schema.

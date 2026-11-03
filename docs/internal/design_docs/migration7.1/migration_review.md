# Notes after reviewing migration reports
##
1. Reviewed initial migration report, in PR comments - raised Issues 1771
2. docs/internal/design_docs/migration7.1/migration_report_bigquery.md reviewed - no comments
3. docs/internal/design_docs/migration7.1/migration_report_oracle.md
  - Reviewed List Pattern Parsing.  Required because SQL Server returns precision for timezone datatype in a different way.
  - why do we need Combiner PyArrow fix. We should be dealing with Decimals as decimals, not converting them!
4. docs/internal/design_docs/migration7.1/migration_report_mssql_jun_5.md 
5. docs/internal/design_docs/migration7.1/migration_report_jun_5.md
6. docs/internal/design_docs/migration7.1/migration_report_jun_5.md
7. docs/internal/design_docs/migration7.1/migration_report_jun_4.md
8. docs/internal/design_docs/migration7.1/migration_report_futureWarn_addl_backends.md
9. docs/internal/design_docs/migration7.1/migration_report_snowflake.md
10. docs/internal/design_docs/migration7.1/migration_report_sybase.md
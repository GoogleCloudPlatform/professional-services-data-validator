# Data Validation of Oracle BLOB

For row hash validations DVT uses the standard hash function available in each SQL engine. For Oracle that is called `STANDARD_HASH()`. Unfortunately that function is incompatible with Oracle BLOB columns:

```
SQL> SELECT STANDARD_HASH(col_blob,'SHA256') FROM dvt_test.tab_blob;
SELECT STANDARD_HASH(col_blob,'SHA256') FROM dvt_test.tab_blob
                     *
ERROR at line 1:
ORA-00902: invalid datatype
```

There is an alternative which is to use the Oracle provided DBMS_CRYPTO package (https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_CRYPTO.html#GUID-4B200807-A740-4A2E-8828-AC0CFF6127D5).

Unfortunately that package is not created by default and, even if it has been created, is not granted to all users. Therefore it is not feasible to integrate this hash function with DVT. Instead this sample code documents how one might hash validate Oracle BLOB data using DVT Customer Query validations (https://github.com/GoogleCloudPlatform/professional-services-data-validator/tree/docs-document-sample-code-for-oracle-blob#custom-query-row-validations).


## Oracle setup

```
sqlplus sys@localhost:1521/service_name as sysdba
@?/rdbms/admin/dbmsobtk.sql
@?/rdbms/admin/prvtobtk.plb
```

Grant execute on DBMS_CRYPTO to your read-only DVT database user:
```
GRANT execute ON DBMS_CRYPTO TO dvt_user;
```


## DBMS_CRYPTO equivalent of STANDARD_HASH

```
SELECT DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW('some binary data'),4)
,      STANDARD_HASH(UTL_RAW.CAST_TO_RAW('some binary data'),'SHA256')
FROM   dual;

DBMS_CRYPTO.HASH(UTL_RAW.CAST_TO_RAW('SOMEBINARYDATA'),4)
--------------------------------------------------------------------------------
STANDARD_HASH(UTL_RAW.CAST_TO_RAW('SOMEBINARYDATA'),'SHA256')
----------------------------------------------------------------
DFB1BCF4EAE00791A2CB06899495F46A3FB9E08467EB81499B5122A0917085A8
DFB1BCF4EAE00791A2CB06899495F46A3FB9E08467EB81499B5122A0917085A8
```

NULL handling is different:
- `DBMS_CRYPTO.HASH` raises an exception for a NULL BLOB input
- `STANDARD_HASH` coerces NULLs to empty string

`EMPTY_BLOB()` is handled as empty string by `DBMS_CRYPTO.HASH`:
```
SELECT DBMS_CRYPTO.HASH((EMPTY_BLOB()),4)
,      STANDARD_HASH(UTL_RAW.CAST_TO_RAW(''),'SHA256')
FROM   dual;

DBMS_CRYPTO.HASH((EMPTY_BLOB()),4)
--------------------------------------------------------------------------------
STANDARD_HASH(UTL_RAW.CAST_TO_RAW(''),'SHA256')
----------------------------------------------------------------
E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855
E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855
```

The key thing is to understand how `EMPTY_BLOB()` was inserted in the target system and cater for it accordingly.

## Oracle to PostgreSQL validation example

This example assumes BLOB data has been copied to PostgreSQL bytea and that any `EMPTY_BLOB()` values in Oracle became NULL in PostgreSQL.

It also assumed by BLOB data in a table called `dvt_test.tab_blob` in a column called `col_blob`.

Oracle query, including the primary key column for matching with target data, and preventing NULLs from reaching DBMS_CRYPTO.HASH():
```
SELECT id,CASE WHEN DBMS_LOB.GETLENGTH(col_blob) = 0 OR col_blob IS NULL THEN NULL ELSE LOWER(DBMS_CRYPTO.HASH(col_blob,4)) END col_blob FROM dvt_test.tab_blob;
```

PostgreSQL query, including the primary key column for matching with source data:
```
SELECT id,encode(sha256(col_blob),'hex') AS col_blob FROM dvt_test.tab_blob;
```

DVT command:
```
data-validation validate custom-query row \
 -sc ora_conn -tc pg_conn \
 --source-query="SELECT id,CASE WHEN DBMS_LOB.GETLENGTH(col_blob) = 0 OR col_blob IS NULL THEN NULL ELSE LOWER(DBMS_CRYPTO.HASH(col_blob,4)) END col_blob FROM dvt_test.tab_blob" \
 --target-query="SELECT id,encode(sha256(col_blob),'hex') AS col_blob FROM dvt_test.tab_blob" \
 --primary-keys=id \
 --comparison-fields=col_blob \
 --format=text
```

The hash values from both systems match, output edited to stack the values:
```
source_column_name | col_blob
source_agg_value   | dfb1bcf4eae00791a2cb06899495f46a3fb9e08467eb81499b5122a0917085a8
target_agg_value   | dfb1bcf4eae00791a2cb06899495f46a3fb9e08467eb81499b5122a0917085a8
validation_status  | success
```

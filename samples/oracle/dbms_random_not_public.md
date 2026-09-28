# Troubleshoot Missing DBMS_RANDOM Access

Data Validator uses `DBMS_RANDOM.VALUE` for some Oracle validations. On systems
that have been hardened for security audits, the default `PUBLIC` grant or public
synonym for `DBMS_RANDOM` may have been removed.

## Symptom

Validations can fail with an error similar to:

```
sqlalchemy.exc.DatabaseError: (cx_Oracle.DatabaseError) ORA-00904:
"DBMS_RANDOM"."VALUE": invalid identifier
```

## Diagnose

Ask a DBA to check the grant and synonym:

```sql
SELECT grantee, owner, table_name, privilege
FROM dba_tab_privs
WHERE table_name = 'DBMS_RANDOM';

SELECT owner, synonym_name, table_owner, table_name
FROM dba_synonyms
WHERE table_name = 'DBMS_RANDOM';
```

By default, Oracle grants `PUBLIC` execute access on `SYS.DBMS_RANDOM` and
provides a public `DBMS_RANDOM` synonym. Either can be absent in a hardened
environment.

## Resolution

Replace `DVT_USER` with the Oracle user configured for the Data Validator
connection. A DBA can grant the required access directly:

```sql
GRANT EXECUTE ON sys.dbms_random TO DVT_USER;
```

If the public synonym has also been removed, create a synonym in the Data
Validator user's schema:

```sql
CREATE SYNONYM DVT_USER.dbms_random FOR sys.dbms_random;
```

Use the least-privilege option appropriate for your organization. These commands
require DBA privileges.

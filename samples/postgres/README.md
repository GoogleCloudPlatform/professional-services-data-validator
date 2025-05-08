# Sample trim_scale UDF for older versions of PostgreSQL

In some circumstances DVT uses the PostgreSQL built in `trim_scale()` function to format decimals in a way compatible with other SQL engines. `trim_scale()` was introduced in PostgreSQL 13 therefore it is possible to encounter the following error when running DVT on older versions of PostgreSQL:

```
sqlalchemy.exc.ProgrammingError: (psycopg2.errors.UndefinedFunction) function trim_scale(numeric) does not exist
```

To workaround this a custom UDF can be created, as described in this [Stack Overflow](https://stackoverflow.com/a/70746714) post. For example:

```
CREATE FUNCTION public.trim_scale(numeric)
    RETURNS numeric
AS $$
SELECT CASE
    WHEN trim($1::text, '0') = '.' THEN 0
    WHEN trim($1::text, '0') = '' THEN 0
    WHEN trim($1::text, '0')::numeric = $1 THEN trim($1::text, '0')::numeric
    ELSE $1 END
$$ LANGUAGE SQL;
```


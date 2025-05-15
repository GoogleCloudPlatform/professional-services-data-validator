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
    WHEN trim($1::text, '0') IN ('.', '') THEN 0
    WHEN trim($1::text, '0')::numeric = $1 THEN trim($1::text, '0')::numeric
    ELSE $1 END
$$ LANGUAGE SQL;
```

## Correctness test
Below I created the trim_scale UDF in a system with PostgreSQL's internal trim_scale function to compare results:
```
CREATE FUNCTION public.trim_scale_dvt(numeric)
    RETURNS numeric
AS $$
SELECT CASE
    WHEN trim($1::text, '0') IN ('.', '') THEN 0
    WHEN trim($1::text, '0')::numeric = $1 THEN trim($1::text, '0')::numeric
    ELSE $1 END
$$ LANGUAGE SQL;

SELECT c, trim_scale(c) pg_trim_scale, public.trim_scale_dvt(c) trim_scale_udf
FROM (
  SELECT UNNEST(ARRAY[
    NULL::decimal,
    0::decimal,
    1::decimal,
    -1::decimal,
    0.0::decimal,
    -0.0::decimal,
    0.0001::decimal,
    0.100100::decimal,
    0.1230000::decimal,
    0.01234567890::decimal,
    -0.01234567890::decimal,
    12345678901234567890::decimal
  ]) AS c
) r;


          c           |    pg_trim_scale     |    trim_scale_udf
----------------------+----------------------+----------------------
                      |                      |
                    0 |                    0 |                    0
                    1 |                    1 |                    1
                   -1 |                   -1 |                   -1
                  0.0 |                    0 |                    0
                  0.0 |                    0 |                    0
               0.0001 |               0.0001 |               0.0001
             0.100100 |               0.1001 |               0.1001
            0.1230000 |                0.123 |                0.123
        0.01234567890 |         0.0123456789 |         0.0123456789
       -0.01234567890 |        -0.0123456789 |        -0.0123456789
 12345678901234567890 | 12345678901234567890 | 12345678901234567890
```

## Performance test

TBC
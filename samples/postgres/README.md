# Sample trim_scale UDF for older versions of PostgreSQL

In some circumstances DVT uses the PostgreSQL built in `trim_scale()` function to format decimals in a way compatible with other SQL engines. `trim_scale()` was introduced in PostgreSQL 13 therefore it is possible to encounter the following error when running DVT on older versions of PostgreSQL:

```
sqlalchemy.exc.ProgrammingError: (psycopg2.errors.UndefinedFunction) function trim_scale(numeric) does not exist
```

To workaround this a custom UDF can be created, as described in this [Stack Overflow](https://stackoverflow.com/a/70746714) post. An example can be found [here](trim_scale.sql).

## Correctness test
Below I created the trim_scale UDF in a system with PostgreSQL's internal trim_scale function to compare results:
```
CREATE FUNCTION dvt_test.trim_scale(numeric)
    RETURNS numeric
AS $$
SELECT CASE
    WHEN trim($1::text, '0') IN ('.', '') THEN 0
    WHEN trim($1::text, '0')::numeric = $1 THEN trim($1::text, '0')::numeric
    ELSE $1 END
$$ LANGUAGE SQL;

SELECT c, trim_scale(c) pg_trim_scale, dvt_test.trim_scale(c) trim_scale_udf
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

This basic performance test generates a table with 5 million rows each containing 10 decimal columns. This step is executed five times so an average elapsed time can be obtained at the end.

A new table is created with the same approach but applying the built in `trim_scale()` function and afterwards an other table is also created in the same way but using the proposed DVT `time_scale()` UDF. This gives us the overhead for each technique.

### Baseline
```
CREATE TABLE trim_scale_tab
AS
SELECT (i::decimal) c1
,      (i+1::decimal) c2
,      (i+2::decimal) c3
,      (i+3::decimal) c4
,      (i+4::decimal) c5
,      (i+5::decimal) c6
,      (i+6::decimal) c7
,      (i+7::decimal) c8
,      (i+8::decimal) c9
,      (i+9::decimal) c10
FROM generate_series(1,5000000) AS t(i);

Time: 12268.008 ms (00:12.268)
Time: 12113.171 ms (00:12.113)
Time: 12875.416 ms (00:12.875)
Time: 13020.033 ms (00:13.020)
Time: 12079.489 ms (00:12.079)
```

Average: 12471ms

### Built in function
```
CREATE TABLE trim_scale_tab
AS
SELECT trim_scale(i::decimal) c1
,      trim_scale(i+1::decimal) c2
,      trim_scale(i+2::decimal) c3
,      trim_scale(i+3::decimal) c4
,      trim_scale(i+4::decimal) c5
,      trim_scale(i+5::decimal) c6
,      trim_scale(i+6::decimal) c7
,      trim_scale(i+7::decimal) c8
,      trim_scale(i+8::decimal) c9
,      trim_scale(i+9::decimal) c10
FROM generate_series(1,5000000) AS t(i);

Time: 19397.326 ms (00:19.397)
Time: 14243.871 ms (00:14.244)
Time: 14296.588 ms (00:14.297)
Time: 13895.630 ms (00:13.896)
Time: 14107.917 ms (00:14.108)
```

Average: 15188ms
Delta: +2717ms (+22%)

### Custom UDF
```
CREATE TABLE trim_scale_tab
AS
SELECT dvt_test.trim_scale(i::decimal) c1
,      dvt_test.trim_scale(i+1::decimal) c2
,      dvt_test.trim_scale(i+2::decimal) c3
,      dvt_test.trim_scale(i+3::decimal) c4
,      dvt_test.trim_scale(i+4::decimal) c5
,      dvt_test.trim_scale(i+5::decimal) c6
,      dvt_test.trim_scale(i+6::decimal) c7
,      dvt_test.trim_scale(i+7::decimal) c8
,      dvt_test.trim_scale(i+8::decimal) c9
,      dvt_test.trim_scale(i+9::decimal) c10
FROM generate_series(1,5000000) AS t(i);

Time: 92619.294 ms (01:32.619)
Time: 66356.498 ms (01:06.356)
Time: 61986.184 ms (01:01.986)
Time: 61201.870 ms (01:01.202)
Time: 61640.150 ms (01:01.640)
```

Average: 68761ms
Delta: +56290ms (+450%)

## Inline CASE expression

```
CREATE TABLE trim_scale_tab
AS
SELECT CASE
    WHEN trim(i::text, '0') IN ('.', '') THEN 0
    WHEN trim(i::text, '0')::numeric = i THEN trim(i::text, '0')::numeric
    ELSE i END c1,
    CASE
    WHEN trim((i+1)::text, '0') IN ('.', '') THEN 0
    WHEN trim((i+1)::text, '0')::numeric = i+1 THEN trim((i+1)::text, '0')::numeric
    ELSE i+1 END c2,
    CASE
    WHEN trim((i+2)::text, '0') IN ('.', '') THEN 0
    WHEN trim((i+2)::text, '0')::numeric = i+2 THEN trim((i+2)::text, '0')::numeric
    ELSE i+2 END c3,
    CASE
    WHEN trim((i+3)::text, '0') IN ('.', '') THEN 0
    WHEN trim((i+3)::text, '0')::numeric = i+3 THEN trim((i+3)::text, '0')::numeric
    ELSE i+3 END c4,
    CASE
    WHEN trim((i+4)::text, '0') IN ('.', '') THEN 0
    WHEN trim((i+4)::text, '0')::numeric = i+4 THEN trim((i+4)::text, '0')::numeric
    ELSE i+4 END c5,
    CASE
    WHEN trim((i+5)::text, '0') IN ('.', '') THEN 0
    WHEN trim((i+5)::text, '0')::numeric = i+5 THEN trim((i+5)::text, '0')::numeric
    ELSE i+5 END c6,
    CASE
    WHEN trim((i+6)::text, '0') IN ('.', '') THEN 0
    WHEN trim((i+6)::text, '0')::numeric = i+6 THEN trim((i+6)::text, '0')::numeric
    ELSE i+6 END c7,
    CASE
    WHEN trim((i+7)::text, '0') IN ('.', '') THEN 0
    WHEN trim((i+7)::text, '0')::numeric = i+7 THEN trim((i+7)::text, '0')::numeric
    ELSE i+7 END c8,
    CASE
    WHEN trim((i+8)::text, '0') IN ('.', '') THEN 0
    WHEN trim((i+8)::text, '0')::numeric = i+8 THEN trim((i+8)::text, '0')::numeric
    ELSE i+8 END c9,
    CASE
    WHEN trim((i+9)::text, '0') IN ('.', '') THEN 0
    WHEN trim((i+9)::text, '0')::numeric = i+9 THEN trim((i+9)::text, '0')::numeric
    ELSE i+9 END c10
FROM generate_series(1,5000000) AS t(i);

Time: 51362.997 ms (00:51.363)
Time: 44526.158 ms (00:44.526)
Time: 43060.392 ms (00:43.060)
Time: 42366.208 ms (00:42.366)
Time: 61329.184 ms (01:01.329)
```

Average: 48529ms
Delta: +36058ms (+289%)

# Floating-point data types converted to string

This document captures some testing of casts of floating-point values to string. We all know data types such as Float and Double are inexact but I wanted to capture an example for future reference.

When running tests I was careful to cast directly from string to the floating-point type to avoid decimal data types from being used and confusing matters further.

By way of example I picked single float and double values and compared the resulting strings with the original input strings. It is likely there are other input values that will produce a different set of variations.

## Tests

### BigQuery

```sql
-- no float32

select cast(cast('12345678.2' as float64) as string);

12345678.2
```

### Db2 LUW

```sql
select to_char(cast('123456.1' as real)) from sysibm.sysdummy1

123456.1015625

select to_char(cast('12345678.2' as double)) from sysibm.sysdummy1"

12345678.199999999
```

### Hive

```sql
select cast(cast('123456.1' as float) as string)

123456.1

select cast(cast('12345678.2' as double) as string)

1.23456782E7
```

### Impala

```sql
select cast(cast('123456.1' as float) as string)

123456.102

select cast(cast('12345678.2' as double) as string)

12345678.199999999
```

### MySQL

```sql
select cast(cast('123456.1' as real) as char);

123456.1

select cast(cast('12345678.2' as double) as char);

12345678.2
```

### Oracle

```sql
select to_char(cast('123456.1' as binary_float),'tm9') from dual;

123456.102

select to_char(cast('12345678.2' as binary_double),'tm9') from dual;

12345678.199999999
```

### PostgreSQL

```sql
select cast(cast('123456.1' as real) as text);

123456.1

select cast(cast('12345678.2' as double precision) as text);

12345678.2
```

### SQL Server

```sql
select format(cast('123456.1' as real), 'G');

123456.1

select format(cast('12345678.2' as float), 'G');

12345678.2
```

### Teradata

```sql
select to_char(cast('123456.1' as float),'tm9');

123456.1

select to_char(cast('12345678.2' as double precision),'tm9');

12345678.2
```

## Results

| System / Input String | FLOAT32 '123456.1' | FLOAT64 '12345678.2' |
| :---- | :---- | :---- |
| BigQuery | N/A | 12345678.2 |
| Db2 LUW | 123456.1015625 | 12345678.199999999 |
| Hive | 123456.1 | 1.23456782E7 |
| Impala | 123456.102 | 12345678.199999999 |
| MySQL | 123456.1 | 12345678.2 |
| Oracle | 123456.102 | 12345678.199999999 |
| PostgreSQL | 123456.1 | 12345678.2 |
| SQL Server | 123456.1 | 12345678.2 |
| Teradata | 123456.1 | 12345678.2 |

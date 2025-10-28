# ClickHouse Validation Guide

This guide shows how to use DVT to validate data between BigQuery and ClickHouse after migration.

## Quick Start

### Prerequisites

- DVT installed with ClickHouse support: `pip install git+https://github.com/alexei-led/professional-services-data-validator.git`
- ClickHouse server accessible on port 9000 (Native TCP protocol)
- Network access through firewalls to port 9000

### Create Connection

```bash
data-validation connections add \
  --connection-name my_clickhouse \
  ClickHouse \
  --host clickhouse.example.com \
  --port 9000 \
  --database analytics \
  --user analyst \
  --password my_password \
  --compression lz4
```

**Important**: Use port **9000** (Native TCP), not port 8123 (HTTP).

### Verify Connection

```bash
data-validation query \
  --conn my_clickhouse \
  --query "SELECT version()"
```

## Validation Workflow

After migrating data from BigQuery to ClickHouse, validate in this order:

### 1. Schema Validation

Compare table structures and identify type differences:

```bash
data-validation validate schema \
  -sc my_bq_conn \
  -tc my_clickhouse \
  -tbls bigquery-project.dataset.orders=analytics.orders
```

**Common type mappings**:

- BigQuery `STRING` → ClickHouse `String`
- BigQuery `INT64` → ClickHouse `Int64`
- BigQuery `FLOAT64` → ClickHouse `Float64`
- BigQuery `TIMESTAMP` → ClickHouse `DateTime` or `DateTime64(3)`
- BigQuery `ARRAY<T>` → ClickHouse `Array(T)`

If you see expected type differences, use `--allow-list`:

```bash
data-validation validate schema \
  -sc my_bq_conn \
  -tc my_clickhouse \
  -tbls bigquery-project.dataset.orders=analytics.orders \
  --allow-list "TIMESTAMP:DateTime64(3),FLOAT64:Float64"
```

### 2. Column Validation

Validate aggregated metrics:

```bash
export TZ=UTC  # CRITICAL for timestamp validation

data-validation validate column \
  -sc my_bq_conn \
  -tc my_clickhouse \
  -tbls bigquery-project.dataset.orders=analytics.orders \
  --count '*' \
  --sum amount,quantity \
  --min order_date \
  --max order_date
```

**With grouping**:

```bash
export TZ=UTC

data-validation validate column \
  -sc my_bq_conn \
  -tc my_clickhouse \
  -tbls bigquery-project.dataset.orders=analytics.orders \
  --grouped-columns region \
  --count '*' \
  --sum amount
```

### 3. Row Validation

Validate individual rows (use filters for large tables):

```bash
export TZ=UTC

data-validation validate row \
  -sc my_bq_conn \
  -tc my_clickhouse \
  -tbls bigquery-project.dataset.orders=analytics.orders \
  --primary-keys order_id \
  --hash '*' \
  --filters 'order_date >= "2024-01-01"'
```

For large tables, generate partitions:

```bash
data-validation generate-table-partitions \
  -sc my_bq_conn \
  -tc my_clickhouse \
  -tbls bigquery-project.dataset.orders=analytics.orders \
  --primary-keys order_id \
  --hash '*' \
  --partition-num 1000 \
  --config-dir partitions/

# Run partitions
data-validation configs run --config-dir partitions/
```

### 4. Custom Query Validation

Validate migrated SQL queries produce identical results.

**BigQuery query** (`bq_query.sql`):

```sql
SELECT
  DATE(order_timestamp) as order_date,
  region,
  COUNT(*) as order_count,
  SUM(amount) as total_revenue
FROM `project.dataset.orders`
WHERE order_timestamp >= '2024-01-01'
GROUP BY order_date, region
```

**ClickHouse query** (`ch_query.sql`):

```sql
SELECT
  toDate(order_timestamp) as order_date,
  region,
  COUNT(*) as order_count,
  SUM(amount) as total_revenue
FROM analytics.orders
WHERE order_timestamp >= '2024-01-01'
GROUP BY order_date, region
```

**Run validation**:

```bash
export TZ=UTC

data-validation validate custom-query column \
  -sc my_bq_conn \
  -tc my_clickhouse \
  -sqf bq_query.sql \
  -tqf ch_query.sql \
  --count order_count \
  --sum total_revenue \
  --grouped-columns order_date,region
```

## Critical Settings

### Timezone

**Always set `TZ=UTC`** when validating timestamps:

```bash
export TZ=UTC
data-validation validate column ...
```

Or inline:

```bash
TZ=UTC data-validation validate column ...
```

Without this, timestamp comparisons will fail due to timezone differences.

### Compression

Enable LZ4 compression for better performance:

```bash
--compression lz4
```

This reduces network bandwidth by 2-5x.

### Storing Results

Store validation results in BigQuery for tracking:

```bash
data-validation validate column \
  -sc my_bq_conn \
  -tc my_clickhouse \
  -tbls bigquery-project.dataset.orders=analytics.orders \
  --count '*' \
  -rh my_bq_conn.pso_data_validator.results \
  --labels migration=bq-to-ch,table=orders
```

## Troubleshooting

### Cannot Connect to ClickHouse

**Error**: Connection refused or timeout

**Solution**: Verify port 9000 is accessible:

```bash
# Test connectivity
nc -zv clickhouse.example.com 9000

# Common mistake: using HTTP port 8123 instead of Native TCP port 9000
# ✗ Wrong: --port 8123
# ✓ Correct: --port 9000
```

Check firewall rules allow port 9000.

### Timestamp Validation Failures

**Error**: Row counts or aggregations don't match, but data looks identical

**Solution**: Set `TZ=UTC`:

```bash
# ✗ Wrong
data-validation validate column ...

# ✓ Correct
export TZ=UTC
data-validation validate column ...
```

Verify timezone in ClickHouse:

```bash
data-validation query \
  --conn my_clickhouse \
  --query "SELECT timezone()"
```

### Schema Type Mismatch

**Error**: Schema validation shows type differences

**Solution**: Use `--allow-list` for expected differences:

```bash
data-validation validate schema \
  -sc my_bq_conn \
  -tc my_clickhouse \
  -tbls bigquery-project.dataset.orders=analytics.orders \
  --allow-list "TIMESTAMP:DateTime64(3),NUMERIC:Decimal(38,9)"
```

### Row Hash Failures

**Error**: Hash validation shows mismatches but data appears identical

**Common causes**:

- Floating point precision differences
- Whitespace differences
- Boolean representation (BigQuery `true`/`false` vs ClickHouse `1`/`0`)

**Solution**: Use `--comparison-fields` instead of `--hash` to identify problematic columns:

```bash
export TZ=UTC

data-validation validate row \
  -sc my_bq_conn \
  -tc my_clickhouse \
  -tbls bigquery-project.dataset.orders=analytics.orders \
  --primary-keys order_id \
  --comparison-fields order_date,amount,status \
  --filters 'order_id < 1000' \
  --filter-status fail
```

Once you identify the problematic column, investigate the data type or precision.

### Slow Validations

**Solutions**:

1. Use compression:

```bash
--compression lz4
```

2. Use filters to reduce data volume:

```bash
--filters 'order_date >= "2024-01-01"'
```

3. Partition large tables:

```bash
data-validation generate-table-partitions --partition-num 1000 ...
```

4. Increase timeout for very large queries:

```bash
--json-params '{"send_receive_timeout": 600}'
```

## Common Validation Patterns

### Validate Multiple Tables

Create a YAML config to validate multiple tables:

```yaml
# validation_config.yaml
source: my_bq_conn
target: my_clickhouse
result_handler:
  type: BigQuery
  project_id: my-project
  table_id: pso_data_validator.results

validations:
  - type: Column
    schema_name: bigquery-project.dataset
    table_name: orders
    target_schema_name: analytics
    target_table_name: orders
    aggregates:
      - type: count
        source_column: null
        target_column: null
      - type: sum
        source_column: amount
        target_column: amount

  - type: Column
    schema_name: bigquery-project.dataset
    table_name: customers
    target_schema_name: analytics
    target_table_name: customers
    aggregates:
      - type: count
        source_column: null
        target_column: null
```

Run:

```bash
TZ=UTC data-validation configs run -c validation_config.yaml
```

### Validate ClickHouse Arrays

ClickHouse supports native Array types:

```bash
data-validation validate column \
  -sc my_clickhouse \
  -tc my_clickhouse \
  -tbls analytics.events \
  --count user_id,event_tags
```

### Validate Distributed Tables

DVT automatically queries through the distributed engine:

```bash
data-validation validate column \
  -sc my_clickhouse \
  -tc my_clickhouse \
  -tbls analytics.orders_distributed \
  --count '*' \
  --sum amount
```

## Additional Resources

- [ClickHouse Connection Setup](connections.md#clickhouse)
- [ClickHouse Examples](examples.md#clickhouse-examples)
- [Sample Validations](../samples/clickhouse/)
- [Installation Guide](installation.md)

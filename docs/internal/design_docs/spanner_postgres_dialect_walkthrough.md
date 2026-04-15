# Spanner PostgreSQL Dialect & Backend Walkthrough

## Overview
The Data Validation Tool (DVT) integrates with Google Cloud Spanner running the PostgreSQL dialect via a custom hybrid architecture under `third_party/ibis/ibis_spanner_postgres`. Because Spanner PostgreSQL acts similarly to standard PostgreSQL but fundamentally has structural differences in data types, functions, and schema handling, we introduced a tailored SQLAlchemy dialect mixed with native Ibis constructs.

## Technical Elements

### 1. Dialect Customization (`dialect.py`)
We implemented a custom SQLAlchemy dialect mixin (`SpannerPostgresDialectMixin`) layered over `PGDialect_psycopg2`.
- **Type Formatting:** Standard PostgreSQL uses the `format_type()` function for reflection. Spanner PostgreSQL does not support this function, so we implement a local conversion map (`PG_TYPE_MAP`) and fallback formatters (`_format_type`).
- **Reflection Safety:** We selectively populate basic relational structure definitions without assuming enums, domains, or arbitrary advanced constraints are fully queryable via the `pg_catalog`.

### 2. Backend Metadata & PK Detection (`__init__.py`)
- **Primary Keys (`list_primary_key_columns`):** Standard PostgreSQL drivers rely on `pg_index` and `pg_attribute` mappings using `CAST(... AS regclass)`. In Spanner PostgreSQL environments, these constructs can break internal CTE translation limits or throw scoping errors. We implemented a bespoke lookup against `information_schema.indexes` and `information_schema.index_columns` matching `index_type = 'PRIMARY_KEY'`.
- **Failed Type Inference Fallback:** Integrates `dvt_handle_failed_column_type_inference` to ensure schema is passed through to `_metadata()` in addition to table name.

### 3. Customized Operations Registry (`registry.py`)
Standard PostgreSQL expressions from Ibis must be filtered or altered to suit Spanner:
- **`ops.StringJoin`:** Employs string concatenation (`||`) since `concat_ws()` operates differently.
- **`ops.TableColumn`:** Strips explicit timezone coercion (`AT TIME ZONE`) that doesn't translate elegantly.
- **`ops.HashBytes`:** Trapped with a `ValueError` alerting that Spanner currently lacks raw byte hashing utilities standard to standard PostgreSQL.
- **`ops.Count`:** Wraps Decimal (numeric) columns in `CAST(col AS String)` before counting, since Spanner PostgreSQL lacks native `count(numeric)` aggregates.

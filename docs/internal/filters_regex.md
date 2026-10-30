# Regular Expressions for filters

## Background

Filters can be a single SQL expression that evaluates to Boolean or two SQL expressions separated by a ':'. The initial implementation used `filters.split(':')`. This works for cases such as `cost > 2` and `cost >2:initial_cost >3`. When the SQL expression contains embedded ':' in timestamp strings, it does not work. A more sophisticated parsing of the SQL expression is needed. 

## Derive the regular expression

* A ':' does not occur in SQL expressions across the different database engines except in the context of bind variables, host variable and variable assignment. ':' can occur in column names in Salesforce, which is not allowed in DVT filters. '::' is a type cast in PostgreSQL, Redshift and Snowflake (e.g. `id::text = '5'`) and is supported, see [Type casts with '::'](#type-casts-with-) below.
* DVT does not need to parse the filter to ensure that it is a valid SQL expression. A filter such as '“column_name > 4' is not a valid SQL expression - since the '“”' around column_name needs to be closed. There is no SQL standard for quoting column names, Oracle uses '"', while BigQuery uses '`'.  When DVT generates code, the backends quote column names correctly. Parsing SQL expressions is too complicated and not necessary. DVT will pass the SQL expression to the database. If there is an error the database will detect and report it.
* The definition for quoted strings in SQL is straightforward - a sequence of characters enclosed in “'“ (single quotes). If a single quote is part of a string, it needs to be doubled. A single quote cannot be escaped with an escape character.
* A regular expression (re) for a sequence of zero or more characters not containing “'“ or “:” is `[^':]*`
* An re for a strings - a sequence of zero of more characters not containing a “‘“ is `'[^']*'`
* An re for a single_filter is `([^':]*('[^']*')*)*`. If separate filters are specified for source and target, the re is `([^':]*('[^']*')*)*:([^':]*('[^']*')*)*`. To extract the source and target filters, the re can be specified as `(?P<source>([^':]*('[^']*')*)*):(?P<target>([^':]*('[^']*')*)*)`.

## Type casts with '::'

DVT splits the filter with `util.split_not_in_quotes`, which only splits on a separator that is outside quoted strings. The separator is a lone colon:

```
(?<!:):(?!:)
```

The lookbehind and lookahead skip any ':' that is next to another ':', so a `::` cast is never treated as the source/target separator. For example `created_at::date > '2024-01-01':CAST(created_at AS DATE) > '2024-01-01'` gives the source filter `created_at::date > '2024-01-01'` and the target filter `CAST(created_at AS DATE) > '2024-01-01'`.

Snowflake JSON path expressions are still not supported. In `src_json:customer:age::INTEGER` the single ':' path operators look exactly like the separator, so the filter is split in the wrong place.

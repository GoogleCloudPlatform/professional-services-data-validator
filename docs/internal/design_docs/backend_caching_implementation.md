# Goal Description

The `raw_column_metadata` method in `third_party/ibis/ibis_db2/__init__.py` (and the Redshift equivalent) executes a `CREATE VIEW` statement and queries the system catalog to retrieve column metadata. For custom queries, this operation is slow and often called multiple times during a single data validation run. The goal is to implement a simple in-memory cache at the `Backend` instance level to store the metadata results, avoiding redundant view creation and system queries.

In the interest of making this caching logic reusable across other database backends in the future, we will extract the core mechanism into a generic caching decorator housed in `third_party/ibis/ibis_addon/api.py`.

## Assumptions
- **Schema Stability**: We safely assume that the structure of the database, tables, or queries will *not* change during a single Data Validation Tool (DVT) run. The initialized `Backend` instance can store the cache for its lifetime without invalidation.
- **Large Cache Keys**: Custom queries could be several kilobytes in length. This is perfectly safe as a dictionary key, as Python natively utilizes highly optimized C-level hashing implementation for strings and strings of this size will pose no noticeable memory or hashing performance penalties.
- **Caching Mechanism**: We considered using `functools.lru_cache` to decorate the function. However, this applies caching globally and permanently ties object instances (`self`) to the global cache key, leading to a memory leak risk if `Backend` instances are recreated. By implementing an instance-level caching decorator, the cache lives and dies cleanly with the exact `Backend` object.

## Proposed Changes

We will create a re-usable caching decorator in `api.py` that manages a dictionary on the `self` object. We will then apply this decorator to the Redshift `raw_column_metadata` method.

### third_party/ibis/ibis_addon/api.py

#### [MODIFY] `api.py` (file:///usr/local/google/home/neiljohnson/github/professional-services-data-validator2/third_party/ibis/ibis_addon/api.py)
- Create a new decorator function `cache_generator_results(func)`.
- The decorator wrapper will check if `self._generator_cache` exists, and initialize an empty dictionary if it doesn't.
- It will construct a cache key combining `func.__name__`, `args`, and a frozen set of `kwargs.items()`.
- If the key is not in the dictionary, it will evaluate the generator by wrapping `func(self, *args, **kwargs)` in a `list()` and store it in `self._generator_cache[key]`.
- Ultimately, it will execute a `yield from self._generator_cache[key]` returning an iterator yielding tuples exactly as the caller expects.

### third_party/ibis/ibis_db2/__init__.py

#### [MODIFY] `__init__.py` (file:///usr/local/google/home/neiljohnson/github/professional-services-data-validator2/third_party/ibis/ibis_db2/__init__.py)
- Import `cache_generator_results` from `third_party.ibis.ibis_addon.api`.
- Simply apply the `@cache_generator_results` decorator to the `Backend.raw_column_metadata` method. This requires zero internal changes to the Backend-specific custom query/view logic!

## Verification Plan

### Automated Tests
- **Unit Testing**: Add a new test case in `tests/unit/ibis_addon/test_api.py` named `test_cache_generator_results`.
  - Create a dummy class with a mocked generator method decorated with `@cache_generator_results`.
  - Assert that calling the method consecutively with identical arguments evaluates the mock method only once and returns the exact same cached list.
  - Assert that invoking the method with novel arguments results in a cache miss and successfully executes the mock method again.
- **System Testing**: Run `pytest tests/system/data_sources/test_db2.py` to ensure no functionality is broken by the caching change.
  - Verify that custom query validations continue to pass and metadata types are correctly resolved.

### Manual Verification
- We can add a temporary log step or use debugger/prints inside `raw_column_metadata` to confirm that the `CREATE VIEW` code is only executed once for identical queries despite multiple yield calls passing through the decorator.

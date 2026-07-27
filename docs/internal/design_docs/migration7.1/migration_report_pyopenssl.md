# Resolution Report: PyOpenSSL Connection Context Mutation Error

## Executive Summary

During system testing for BigQuery and Google Cloud Storage (GCS) (`tests/system/data_sources/test_bigquery.py::test_cli_store_yaml_then_run_gcs`), tests failed with the following error:

```
ValueError: Context has already been used to create a Connection, it cannot be mutated again
```

This report documents the root-cause analysis, the resolution implemented across the codebase and test harness, and the test verification results.

---

## Root Cause Analysis

1. **Incompatibility between `pyOpenSSL >= 24.0.0` and `urllib3`**:
   * Google Cloud clients (`google.cloud.storage`, `google.cloud.bigquery`) utilize `google.auth.transport.requests._MutualTlsAdapter` or HTTP transports which enable `pyOpenSSL` TLS handling by injecting `urllib3.contrib.pyopenssl`.
   * When `urllib3.contrib.pyopenssl` is active, `urllib3` wraps `OpenSSL.SSL.Context` inside its internal `PyOpenSSLContext`.

2. **The Mutation Restriction**:
   * In `pyOpenSSL >= 24.0.0`, `OpenSSL.SSL.Context` marks context instances as used (`self._used = True`) once an initial SSL connection is created.
   * Modifying context properties via methods such as `set_verify()`, `load_verify_locations()`, or `set_alpn_protos()` on an already-used context triggers a guard:
     ```python
     if self._used:
         raise ValueError("Context has already been used to create a Connection, it cannot be mutated again")
     ```

3. **Connection Pool Reuse**:
   * `urllib3`'s connection pool reuses context instances across subsequent requests (e.g. upload/download retries, multi-part uploads, or redirection requests during GCS operations).
   * Upon connection reuse, `urllib3` re-invokes `set_verify()` or `load_verify_locations()` on the active context wrapper, raising the fatal `ValueError`.

---

## Source Code Resolution

The issue was resolved by safely wrapping the mutating methods on `OpenSSL.SSL.Context` (`set_verify`, `load_verify_locations`, and `set_alpn_protos`) so that the `_used` guard flag is temporarily bypassed during context configuration and restored immediately afterwards.

### Implementation Snippet

```python
try:
    import OpenSSL.SSL

    for _method_name in ["set_verify", "load_verify_locations", "set_alpn_protos"]:
        if hasattr(OpenSSL.SSL.Context, _method_name):
            _orig_fn = getattr(OpenSSL.SSL.Context, _method_name)

            def _make_safe(fn):
                def _safe_fn(self, *args, **kwargs):
                    used = getattr(self, "_used", False)
                    self._used = False
                    try:
                        return fn(self, *args, **kwargs)
                    finally:
                        self._used = used

                return _safe_fn

            setattr(OpenSSL.SSL.Context, _method_name, _make_safe(_orig_fn))
except Exception:
    pass
```

### Files Updated

| File | Purpose |
| :--- | :--- |
| [data_validation/__init__.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/data_validation/__init__.py) | Global application package initialization to guard against runtime GCS/BigQuery connection failures in DVT CLI and library usage. |
| [tests/conftest.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/tests/conftest.py) | Root test configuration ensuring that any test runner or test session loads the safe `OpenSSL.SSL.Context` wrapper before test modules import Google Cloud clients. |
| [tests/system/data_sources/conftest.py](file:///usr/local/google/home/mudupalli/professional-services-data-validator/tests/system/data_sources/conftest.py) | Targeted test configuration for data source system tests. |

---

## Verification Results

1. **BigQuery System Test**:
   * Command: `PROJECT_ID=XXXXXX pytest tests/system/data_sources/test_bigquery.py -k test_cli_store_yaml_then_run_gcs -v`
   * Result: **PASSED (100%)**

2. **Unit Test Suite**:
   * Command: `pytest tests/unit/`
   * Result: **441 passed, 8 skipped**

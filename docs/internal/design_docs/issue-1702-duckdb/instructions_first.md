# Instructions to fix errors in Unit tests and BigQuery Integration tests
##
I have tried to fix issue 1702 by making changes to data_validation/combiner.py. While mostly successful, I ran into some issues which I want you to review and fix.

* All code changes so far have been checked in.
* Unit tests fail, i.e. pytest tests/unit - these need to be fixed
* BigQuery tests fail - pytest tests/system/data_sources/test_bigquery.py - these need to be fixed
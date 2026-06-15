# Instructions to convert to using duckdb instead of the ibis pandas backend.
## Objective
Data Validation Tool (DVT) uses Ibis as its query-building engine. For local files and in-memory validations (e.g., CSV, JSON, Parquet, and ORC files), DVT historically defaults to the **Pandas** backend. Also the combiner uses Pandas dataframes for storing the results. We have to replace Pandas dataframes with DuckDB tables for storing the results.

However, the **DuckDB** backend is significantly faster, uses less memory, and natively supports rich SQL queries, making it a superior choice for file-based validations.
Now that Postgres integration tests are passing here are instructions to update the code to use the duckdb backend for pandas dataframes instead of using ibis.pandas.connect().
## Making the changes
* Do not use the ibis.pandas.connect() to refer to a pandas dataframe as an Ibis table
* Instead use the ibis.backends.duckdb.backend.connect(dataframe) to create Ibis table from pandas dataframe. For example, if the df is a pandas dataframe, then 
```python
import ibis
import duckdb
backend = ibis.backends.duckdb.backend.connect()
df = pd.DataFrame(...) # pandas dataframe
ibis_table = backend.create_table("ibis_table", df)
``` 
* Once the code changes are done, run the Postgres integration tests as outlined below to ensure that we have not introduced any regressions.
## How to run the integration tests
* You have to use the Python virtual environment by running `source env/bin/activate` from the shell.
* Prompt me for the values of the environment variables PROJECT_ID and POSTGRES_PASSWORD. 
* Run the integration tests as follows `pytest tests/system/data_sources/test_postgres.py`

## Suggested Steps
* Run the tests as mentioned above
* Review list of failures to identify the cause. Sometimes multiple failures may be due to a single root cause. Fix the root cause and rerun the tests.
* If all tests pass, this step is complete.

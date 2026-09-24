# Goal Description

The [combiner](data_validation/combiner.py) generates the validation report based on the results of the SQL queries on the source and target tables. The results of the SQL queries are provided along with some other parameters (not the whole config manager for some reason). The code is obscure and it is not exactly clear what it does. If we have to migrate beyond Ibis 10.0, we need to stop using the pandas connector the code relies on at the moment.

# Problem Definition
DVT generates SQL queries that are expected to produce identical results if the source and target tables are identical. If the queries return different results, then the tables are likely different. Generating the validation report should be fairly straightforward. There are three different types of validations.

## Basic Column Validations
These are validations where the table columns are evaluated in aggregate (count, min, max, sum etc) and the aggregates are compared. DVT produces a SQL query that generates one row with a column for each of the aggregates requested by the user. In some cases the aggregates are scalars (avg), where it may be acceptable to mix different types (e.g. decimal vs float). Generating a validation report should be fairly simple.

## Grouped Column Validations
This is a variation of the basic column validation, except that the rows in the source and target tables are grouped by one (one only?) column. An example might be when a table contains sales data and the aggregates are grouped by month (or storeID). The results from the source and target contain multiple rows. When both results are compared on the same value of the grouped column, this is same as a Basic Column Validation. This is a variation of basic column validation.

## Row Validations
In a row validation, each row in the source and target table are returned based on the primary keys provided. With hash validation and concat-fields, one aggregate value of all the columns in the row is returned. The report generation in this case is likely straightforward. There is an option --comparison-fields - I am not clear about the structure of the report to be generated.

# Related Comments
## Recursive Validation is dead code
The [data_validation](data_validation/data_validation.py) contains a function called `execute_recursive_validation` which is mostly dead code. If grouped_fields is Null, then the function calls `_execute_validation`. grouped_fields is set from config manager. Looking through the code the grouped fields is from the command line parameter `--grouped-columns` which are only usable with column validations. So most of this code is useless and the function should be factored out. 
## Pandas is type flexible while SQL is strongly typed
The combiner runs SQL queries on pandas dataframes. This results in lot of complexity in converting from a strongly typed data (from the database engine) to Pandas dataframe (loosely typed) and using SQL. Ibis supports pyArrow which is strongly typed and support some simple SQL functions on its own. It might be easier to receive the data as a PyArrow table and generate the report which can be converted to a Pandas dataframe.
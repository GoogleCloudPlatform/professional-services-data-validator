# Partition Table 

## Why Partition ?
Data-validation tool performs row validation by comparing every row in the source database with the corresponding row in the target database. Since the comparison is done in memory, all the rows have to be in memory. Databases typically have a large number of rows that don't all fit in memory, therefore Data-validation tool can run into MemoryError and fail. One way to address this error is to partitition the source and target table into several corresponding partititions. Then validate the corresponding source and target partitions one at a time (either sequentially or in parallel). If the validation is performed in parallel on multiple VMs or containers, this approach has the added benefit of speeding up data validation. There are other approaches available to address the MemoryError - see future work below. Column validation and schema validation do not bring each row of the table into memory. Therefore, they do not run into MemoryErrors.

## How to paritition?
Data-validation tool matches rows based on the specified primary key(s). If we split up the table into small enough partitions, then each partition can be validated without running into MemoryError. This can be depicted pictorially as shown below:
![Alt text](./partition_picture.png?raw=true "Title")
Here, both tables are sorted by primary key(s). The blue line denotes the first row of each partition. With tables partitioned as shown, the complete tables can be row validated by concatenating the results of validating each partition source table against the corresponding partition of the target table.
### What filter clause to use?
Data-validation tool has an option in the row validation command for a ```--filters``` parameter which allows a WHERE statement to be applied to the query used to validate. This parameter can be used to partition the table. The filter clauses are:
1. For the first partition, it is every row with primary key(s) value smaller than the first row of second partition
2. For the last partition, it is every row with primary key(s) value larger than or equal to the the first row of last partition.
3. For all other partitions, it is every row with primary key(s) value larger than or equal to to the first row of the current partition *AND* with primary key(s) values less than the first row of the next partition.
### How to calculate the first row of each partition?
SQL has an analytic function called NTILE which divides an ordered data set into the specified number of buckets and assigns the appropriate bucket number to each row. SQL has an analytic function FIRST_VALUE which returns the first value in an ordered set of values. Both of these can be used to calculate the value of the primary key(s) of the first row of a partition. The following SQL statement gets the value of the primary key(s) for the first row of each partition:
```
SELECT   DISTINCT
            first_value(primary_key_1) OVER ntile_window  AS first_pk1,
            first_value(primary_key_2) OVER ntile_window AS first_pk2,
            ....
            partition_no
                FROM   (
                    SELECT   primary_key_1,
                        primary_key_2,
                        ....,
                        ntile(5) OVER (ORDER BY primary_key_1, primary_key_2, .... ) partition_no
                        FROM     <database_table>)
WINDOW ntile_window AS (partition BY partition_no ORDER BY primary_key_1, primary_key_2, .... ROWS BETWEEN UNBOUNDED  PRECEDING AND UNBOUNDED FOLLOWING)
ORDER BY partition_no ASC;
```
The internal select statement adds the partition number to each row in the table and the external select statement gets the value of the primary keys for the first row. 
## Future Work
### How many partitions do I need?
Partition table requires that the user decide on the number of partitions into which they need to divide the table to avoid MemoryError. Data-validation tool can run on different VMs with different shapes, so the number of partitions depends on the amount of memory available. How does the user figure out the number of partitions they need? Right now, it is by trial and error, say start with 10, then try 100, try to see if 50 still results in MemoryError etc. This is not optimal. Python's ```psutil``` package has a function [virtual_memory()](https://psutil.readthedocs.io/en/latest/#psutil.virtual_memory) which tell us the total and available memory. ```generate-table-partitions``` is provided with all the parameters used in ```validate row```, and the memory grows linearly to the number of rows being validated. ```generate-table-partitions``` can bring say 10,000 rows into memory as though performing a row validation. Using the virtual_memory() function in ```psutil```, ```generate-table-partitions``` can estimate the number of rows that will fit in memory for row validation. Since we can calculate the total number of rows, we can estimate the number of partitions needed. This may need some experimentation, as we may need to allow for memory usage by other functions/objects in Data-validation tool.
### Can Data-validation tool run without MemoryError?
The above paragraph suggests that Data-validation tool can bring in a limited number of rows into memory at a time, perform row validation and avoid MemoryError. This is certainly possible and is complicated. If every row in the source has a corresponding row in the target (and vice versa), and the source and target table are sorted, then ```validate row``` can read a fixed number of rows from both source and target tables into memory and perform row validation and repeat until all rows have been processed. There may not be a corresponding row in the target for every row in the source, so the target may have additional rows in memory for which the corresponding rows in the source table are in the next partition of rows. Therefore validating in this situation without MemoryError can be challenging. ```generate-table-partitions``` allows parallelization of data validation, so it is a useful function to have even if Data-validation tool is modified to validate rows without MemoryError.
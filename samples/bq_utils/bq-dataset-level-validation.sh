# get all tables from source dataset and save them in a temporary CSV file
bq ls --max_results 100 $1:$2 | tail -n +3 | tr -s ' ' | cut -d' ' -f2 > source_tables.csv

# create BQ connection for source
data-validation connections add \
    --connection-name my_bq_conn_source BigQuery \
    --project-id $1

# create BQ connection for target 
data-validation connections add \
    --connection-name my_bq_conn_target BigQuery \
    --project-id $3

input="./source_tables.csv"
# perform both column and schema validation for every table in the given dataset
while IFS= read -r table
do
  command="data-validation validate column -sc my_bq_conn_source -tc my_bq_conn_target -bqrh $5 -tbls $1.$2.$table=$3.$4.$table ${@:6}"
  eval "$command"
  
  command="data-validation validate schema -sc my_bq_conn_source -tc my_bq_conn_target -bqrh $5 -tbls $1.$2.$table=$3.$4.$table"
  eval "$command"
done < "$input"

# delete the temporary file
rm $input


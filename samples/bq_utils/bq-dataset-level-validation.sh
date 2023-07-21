# get all tables from source dataset and save them as tables.csv
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
while IFS= read -r line
do
  sentence="data-validation validate column -sc my_bq_conn_source -tc my_bq_conn_target -bqrh $5 -tbls $1.$2.$line=$3.$4.$line ${@:6}"
  eval "$sentence"
  
  sentence="data-validation validate schema -sc my_bq_conn_source -tc my_bq_conn_target -bqrh $5 -tbls $1.$2.$line=$3.$4.$line"
  eval "$sentence"
done < "$input"


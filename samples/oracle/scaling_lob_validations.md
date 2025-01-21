# Scaling Out Oracle LOB Validations

This sample shows how row hash validations of Oracle BLOBs can be partitioned to minimize memory requirements and scale processing horizontally. The same principles apply to Oracle CLOBs.

## Test table
Below is the DDL for the 4 million row test table used in this sample.

The contents of the BLOB column are not relevant to this test because we are assessing DVT functionality and DVT only has to deal with hash values and not the column contents. However you should bear in mind that queries over large LOB columns may be resource intensive in the Oracle instance.

```sql
CREATE TABLE dvt_test.tab_blob_4m
( id       NUMBER(8) PRIMARY KEY
, col_blob BLOB)
PCTFREE 0;

INSERT INTO dvt_test.tab_blob_4m
SELECT CAST(ROWNUM AS NUMBER(8))
,      UTL_RAW.CAST_TO_RAW('DVT')
FROM   dual
CONNECT BY ROWNUM <= 2000000;
INSERT INTO dvt_test.tab_blob_4m
SELECT id+2000000, col_blob FROM dvt_test.tab_blob_4m;
COMMIT;
```

## Serial test
The commands below are based on [Oracle LOB validations](lob_validations.md).

```sh
Q="SELECT id,CASE WHEN DBMS_LOB.GETLENGTH(col_blob) = 0 OR col_blob IS NULL THEN NULL ELSE LOWER(DBMS_CRYPTO.HASH(col_blob,4)) END col_blob FROM dvt_test.tab_blob_4m"

time data-validation validate custom-query row \
 -sc ora23_gcp -tc ora23_gcp \
 --primary-keys=id \
 --source-query="${Q}" \
 --target-query="${Q}" \
 --comparison-fields=col_blob \
 --format=csv > /tmp/col_blob.csv
```

The job took 3 minutes to complete and Linux showed a memory usage delta of approx 6.5GB and peak CPU of 100% of a single CPU thread.

## Scaling horizontally
Based on the serial test above we can see that we are limited by DVT only being able to consume a single CPU. We can also see that if the table contains 100s of millions of rows then available RAM would become a limiting factor.

### Generate partition YAML files
Even though we must use custom queries for BLOB validations we can still partition the validation.

This sample splits the validation into 20 partitions, each processing 200,000 rows. Because there is a startup overhead for each task in Cloud Run `--parts-per-file` has been used to include 2 validations per configuration file:

```sh
BUCKET=dvt-bucket
export PSO_DV_CONN_HOME=gs://${BUCKET}/conn

Q="SELECT id,CASE WHEN DBMS_LOB.GETLENGTH(col_blob) = 0 OR col_blob IS NULL THEN NULL ELSE LOWER(DBMS_CRYPTO.HASH(col_blob,4)) END col_blob FROM dvt_test.tab_blob_4m"

data-validation generate-table-partitions \
 -sc ora23_gcp \
 -tc ora23_gcp \
 --primary-keys=id \
 --source-query="${Q}" \
 --target-query="${Q}" \
 --comparison-fields=col_blob \
 --format=csv \
 --config-dir gs://${BUCKET}/col_blob/config \
 --partition-num 20 \
 --parts-per-file=2
```

Output shows it only took a couple of seconds to partition the 4 million row table:
```
01/17/2025 05:27:12 PM-INFO: Writing table partition configs to directory: gs://dvt-bucket/col_blob/config
01/17/2025 05:27:13 PM-INFO: Success! Table partition configs written to directory: gs://dvt-bucket/col_blob/config
```

### Execute via Cloud Run
The commands below execute the validation over the 10 configuration files in 4 parallel streams:

```sh
PROJECT=my-project
LOCATION=my-region
REPO=dbbb-dvt
BUCKET=dvt-bucket
NETWORK=net
PSO_DV_CONN_HOME=gs://${BUCKET}/conn
YAML_DIR="gs://${BUCKET}/col_blob/config/custom.caudu"
JOB_NAME="dvt-$(date +'%Y%m%d%H%M%S')"

gcloud run jobs create ${JOB_NAME} \
  --project ${PROJECT} --region ${LOCATION} --network=${NETWORK} \
  --image ${LOCATION}-docker.pkg.dev/${PROJECT}/${REPO}/dvt:640 \
  --tasks 10 --max-retries 1 --parallelism 4 \
  --task-timeout=900s --execute-now \
  --memory=2Gi \
  --set-env-vars PSO_DV_CONN_HOME=${PSO_DV_CONN_HOME} \
  --args="configs,run,-kc,-cdir=${YAML_DIR}"

gcloud run jobs executions list --job ${JOB_NAME} \
--project ${PROJECT} --region ${LOCATION}
```

From the Cloud Run console we can see that all tasks are complete with an elapsed time of 2 minutes:
```
Execution ID             Creation time            Tasks           End time
------------------------ ------------------------ --------------- ------------------------
dvt-20250117173257-8zspt Jan 17, 2025, 5:32:59 PM 10/10 completed Jan 17, 2025, 5:35:10 PM
```

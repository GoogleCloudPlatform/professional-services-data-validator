#!/bin/bash
set -e

# Variables
PROJECT_ID="pso-kokoro-resources"
REGION="us-central1"
NETWORK="default"
BUCKET_NAME="[YOUR_GCS_BUCKET_NAME]" # Replace with your bucket

# New Instance Names
NEW_MYSQL_INSTANCE="data-validator-mysql-v8"
NEW_POSTGRES_INSTANCE="data-validator-postgres-v16"
NEW_MSSQL_INSTANCE="data-validator-mssql-v2022"

# Old Instance Names
OLD_MYSQL_INSTANCE="data-validator-mysql"
OLD_POSTGRES_INSTANCE="data-validator-postgres12"
OLD_MSSQL_INSTANCE="data-validator-mssql2017"

# Databases
MYSQL_DB="pso_data_validator"
POSTGRES_DB="guestbook"
MSSQL_DB="guestbook"

# Versions
MYSQL_VERSION="MYSQL_8_0"
POSTGRES_VERSION="POSTGRES_16"
MSSQL_VERSION="SQLSERVER_2022_STANDARD"

echo "Creating new Cloud SQL instances..."

# Create MySQL
gcloud sql instances create $NEW_MYSQL_INSTANCE \
    --database-version=$MYSQL_VERSION \
    --tier=db-n1-standard-1 \
    --region=$REGION \
    --project=$PROJECT_ID \
    --no-assign-ip \
    --network=$NETWORK

# Create Postgres
gcloud sql instances create $NEW_POSTGRES_INSTANCE \
    --database-version=$POSTGRES_VERSION \
    --tier=db-custom-1-3840 \
    --region=$REGION \
    --project=$PROJECT_ID \
    --no-assign-ip \
    --network=$NETWORK

# Create MSSQL
# Note: SQL Server usually requires setting a root password.
# You might need to add --root-password flag or set it later.
gcloud sql instances create $NEW_MSSQL_INSTANCE \
    --database-version=$MSSQL_VERSION \
    --tier=db-custom-2-7680 \
    --region=$REGION \
    --project=$PROJECT_ID \
    --no-assign-ip \
    --network=$NETWORK

echo "Instances created. Now backing up existing databases..."

# We need to ensure the service accounts have access to the bucket.
OLD_MYSQL_SA=$(gcloud sql instances describe $OLD_MYSQL_INSTANCE --project=$PROJECT_ID --format="value(serviceAccountEmailAddress)")
OLD_POSTGRES_SA=$(gcloud sql instances describe $OLD_POSTGRES_INSTANCE --project=$PROJECT_ID --format="value(serviceAccountEmailAddress)")
OLD_MSSQL_SA=$(gcloud sql instances describe $OLD_MSSQL_INSTANCE --project=$PROJECT_ID --format="value(serviceAccountEmailAddress)")

NEW_MYSQL_SA=$(gcloud sql instances describe $NEW_MYSQL_INSTANCE --project=$PROJECT_ID --format="value(serviceAccountEmailAddress)")
NEW_POSTGRES_SA=$(gcloud sql instances describe $NEW_POSTGRES_INSTANCE --project=$PROJECT_ID --format="value(serviceAccountEmailAddress)")
NEW_MSSQL_SA=$(gcloud sql instances describe $NEW_MSSQL_INSTANCE --project=$PROJECT_ID --format="value(serviceAccountEmailAddress)")

echo "Please ensure the following service accounts have Storage Object Admin or Creator/Viewer access to gs://$BUCKET_NAME:"
echo "Old MySQL SA: $OLD_MYSQL_SA"
echo "Old Postgres SA: $OLD_POSTGRES_SA"
echo "Old MSSQL SA: $OLD_MSSQL_SA"
echo "New MySQL SA: $NEW_MYSQL_SA"
echo "New Postgres SA: $NEW_POSTGRES_SA"
echo "New MSSQL SA: $NEW_MSSQL_SA"

echo "You can grant access with:"
echo "gcloud storage buckets add-iam-policy-binding gs://$BUCKET_NAME --member=serviceAccount:$OLD_MYSQL_SA --role=roles/storage.objectAdmin"
echo "gcloud storage buckets add-iam-policy-binding gs://$BUCKET_NAME --member=serviceAccount:$OLD_POSTGRES_SA --role=roles/storage.objectAdmin"
echo "gcloud storage buckets add-iam-policy-binding gs://$BUCKET_NAME --member=serviceAccount:$OLD_MSSQL_SA --role=roles/storage.objectAdmin"
echo "gcloud storage buckets add-iam-policy-binding gs://$BUCKET_NAME --member=serviceAccount:$NEW_MYSQL_SA --role=roles/storage.objectAdmin"
echo "gcloud storage buckets add-iam-policy-binding gs://$BUCKET_NAME --member=serviceAccount:$NEW_POSTGRES_SA --role=roles/storage.objectAdmin"
echo "gcloud storage buckets add-iam-policy-binding gs://$BUCKET_NAME --member=serviceAccount:$NEW_MSSQL_SA --role=roles/storage.objectAdmin"

echo "Press enter to continue after granting permissions..."
read

# Export
echo "Exporting databases..."
gcloud sql export sql $OLD_MYSQL_INSTANCE gs://$BUCKET_NAME/mysql_backup.sql \
    --database=$MYSQL_DB \
    --project=$PROJECT_ID

gcloud sql export sql $OLD_POSTGRES_INSTANCE gs://$BUCKET_NAME/postgres_backup.sql \
    --database=$POSTGRES_DB \
    --project=$PROJECT_ID

gcloud sql export bak $OLD_MSSQL_INSTANCE gs://$BUCKET_NAME/mssql_backup.bak \
    --database=$MSSQL_DB \
    --project=$PROJECT_ID

# Create databases in new instances
echo "Creating databases in new instances..."
gcloud sql databases create $MYSQL_DB --instance=$NEW_MYSQL_INSTANCE --project=$PROJECT_ID
gcloud sql databases create $POSTGRES_DB --instance=$NEW_POSTGRES_INSTANCE --project=$PROJECT_ID

# Import
echo "Importing databases..."
gcloud sql import sql $NEW_MYSQL_INSTANCE gs://$BUCKET_NAME/mysql_backup.sql \
    --database=$MYSQL_DB \
    --project=$PROJECT_ID

gcloud sql import sql $NEW_POSTGRES_INSTANCE gs://$BUCKET_NAME/postgres_backup.sql \
    --database=$POSTGRES_DB \
    --project=$PROJECT_ID

gcloud sql import bak $NEW_MSSQL_INSTANCE gs://$BUCKET_NAME/mssql_backup.bak \
    --database=$MSSQL_DB \
    --project=$PROJECT_ID

echo "Databases restored."

echo "Updating cloudbuild.yaml..."
sed -i "s/data-validator-mysql/data-validator-mysql-v8/g" cloudbuild.yaml
sed -i "s/data-validator-postgres12/data-validator-postgres-v16/g" cloudbuild.yaml
sed -i "s/data-validator-mssql2017/data-validator-mssql-v2022/g" cloudbuild.yaml

echo "Done. Please review cloudbuild.yaml changes."

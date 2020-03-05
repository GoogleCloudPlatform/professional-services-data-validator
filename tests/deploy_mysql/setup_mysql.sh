#!/bin/bash
##### DEPLOY MASTER ON PUBLIC INSTANCE ######
source BUILD.env

# Push SQL to GCP
gsutil mb $GS_BUCKET
gsutil cp create_db.sql $CREATE_DB_SQL
gsutil acl ch -g AllUsers:R $CREATE_DB_SQL

gsutil cp load_new_data.sql $LOAD_ROW_SQL
gsutil acl ch -g AllUsers:R $LOAD_ROW_SQL

# Spin Up MySQL Cliud SQL Instance
## Create DB guestbook, Table guestbook.entries, and User rep_user
echo Creating New SQL Master $SQL_MASTER
gcloud sql instances create $SQL_MASTER --project $PROJECT_ID --zone $ZONE --tier $SQL_MASTER_TIER --root-password $SQL_MASTER_ROOT_PSWD --backup --enable-bin-log

# Enable supplied IP
# TODO: This should be done during Migration Setup using replica IP
echo Patch $SQL_MASTER to Allow All Traffic -- eek
gcloud sql instances patch mysql-db --authorized-networks=$GKE_IPS --quiet

# Load Data Into Master
echo Load Some Data Into $SQL_MASTER
gcloud sql import sql $SQL_MASTER $CREATE_DB_SQL --project $PROJECT_ID --quiet


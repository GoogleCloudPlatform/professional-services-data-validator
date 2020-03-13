#!/bin/bash
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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


#!/bin/bash

gcloud builds submit --tag gcr.io/${PROJECT_ID}/data-validation \
    --project=${PROJECT_ID}
gcloud run deploy data-validation --image gcr.io/${PROJECT_ID}/data-validation \
    --region=us-central1 --project=${PROJECT_ID}

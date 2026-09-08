#!/bin/bash
set -e

if [ -z "$PROJECT_ID" ]; then
    echo "Error: PROJECT_ID environment variable is not set."
    exit 1
fi

REGION="${REGION:-us-central1}"
REPO="${REPO:-dvt}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/dvt:latest"

echo "Building image: $IMAGE"
gcloud builds submit --tag "$IMAGE" --project="${PROJECT_ID}"

echo "Deploying to Cloud Run..."
gcloud run deploy dvt \
    --image "$IMAGE" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --no-allow-unauthenticated

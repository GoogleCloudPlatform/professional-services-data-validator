# Data Validation on Kubernetes Pod Operator


### Requirements to run the DAG:
- Pre-existing Composer v2 environment
- Build DVT image given below instructions

### Instructions

1. Download the DAG file in this directory
2. Update the DAG configuration for your use case (update PROJECT_ID, optional gcs bucket)
3. Upload it to the DAGs folder in your Composer environment

### Build Docker Image

You will need to build a Docker image to be used by Kubernetes Pod Operator. In order to add
Teradata or Oracle, you will need to customize the Dockerfile and add your
licensed utilities.

```
export PROJECT_ID=<PROJECT-ID>
gcloud builds submit --tag gcr.io/${PROJECT_ID}/data-validation \
    --project=${PROJECT_ID}
```

terraform {
  backend "gcs" {
    bucket  = "pso-kokoro-resources-terraform"
    prefix  = "github/GoogleCloudPlatform/professional-services-data-validator"
  }
}

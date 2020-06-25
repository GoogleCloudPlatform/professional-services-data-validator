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

provider "google" {}

resource "google_bigquery_dataset" "default" {
  project = var.project_id
  dataset_id = "pso_data_validator"
  location = "US"
}

resource "google_bigquery_table" "default" {
  project = var.project_id
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id = "results"
  schema = file("${path.module}/results_schema.json")
  time_partitioning {
      field = "start_time"
      type = "DAY"
  }
  clustering = [
    "validation_name",
    "run_id",
  ]
}

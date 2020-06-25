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

# Terraform backend to manage state for test environment. Delete this file to
# use local Terraform state (for example in a development environment).

terraform {
  backend "gcs" {
    bucket  = "pso-kokoro-resources-terraform"
    prefix  = "github/GoogleCloudPlatform/professional-services-data-validator"
  }
}

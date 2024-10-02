# Copyright 2021 Google LLC
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

from google.api_core import exceptions


class SecretManagerBuilder:
    def build(self, client_type):
        """
        :param client_type:
        :return: secret manager instance currently support gcp secret manager
        """
        if client_type.lower() == "gcp":
            return GCPSecretManager()
        else:
            raise Exception(f"{client_type} is not supported yet.")


class GCPSecretManager:
    """
    GCPSecretManager: client to access secrets stored at GCP secret manager
    """

    def __init__(self):
        # Import the Secret Manager client library.
        from google.cloud import secretmanager

        # Create the Secret Manager client.
        self.client = secretmanager.SecretManagerServiceClient()

    def maybe_secret(self, project_id: str, secret_id: str, version_id="latest") -> str:
        """Get information about the given secret.

        Returns:
            str: Secret value as a string or the secret id if the secret does not exist.
        """
        try:
            # Build the resource name of the secret.
            name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
            # Access the secret version.
            response = self.client.access_secret_version(name=name)
            # Return the decoded payload.
            payload = response.payload.data.decode("UTF-8")
            return payload
        except exceptions.NotFound:
            # The secret does not exist and therefore we can assume it is just a token to be returned.
            return secret_id

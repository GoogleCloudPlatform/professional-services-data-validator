class SecretMangerBuilder:
    def build(self, client_type):
        """
        :param client_type:
        :return: secret manger instance currently support gcp secret manger
        """
        if client_type.lower() == "gcp":
            return GCPSecretManger()
        else:
            raise Exception(f"{client_type} is not supported yet.")


class GCPSecretManger:
    """
    GCPSecretManger: client to access secrets stored at GCP secret manger
    """

    def __init__(self):
        # Import the Secret Manager client library.
        from google.cloud import secretmanager

        # Create the Secret Manager client.
        self.client = secretmanager.SecretManagerServiceClient()

    def maybe_secret(self, project_id, secret_id, version_id="latest"):
        """
        Get information about the given secret.
        :return String value with the secret value or the secret id if the secret value if not exists
        """
        try:
            # Build the resource name of the secret.
            name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
            # Access the secret version.
            response = self.client.access_secret_version(name=name)
            # Return the decoded payload.
            payload = response.payload.data.decode("UTF-8")
            return payload
        except Exception as e:
            print(e)
            return secret_id


if __name__ == "__main__":
    import os

    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"
    ] = "/Users/moukhtar/ws/ma-sabre-sandbox-01-dfe8f33bb3ad.json"
    print(
        SecretMangerBuilder()
        .build("GCP")
        .maybe_secret("ma-sabre-sandbox-01", "db_user")
    )

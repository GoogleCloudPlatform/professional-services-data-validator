import os
import pytest
from data_validation import secret_manager


project_id = os.getenv("project_id")


def test_secret_manager_not_supported():
    """Test build secret manger."""
    with pytest.raises(Exception) as ex:
        client_type = "test_manager"
        secret_manager.SecretManagerBuilder().build(client_type)
    assert f"{client_type} is not supported yet." == str(ex.value)


def test_access_gcp_secret_exists():
    """Test maybeSecret with a secret exists."""
    client_type = "gcp"
    secret_id = "db_test_user"
    expected_secret_value = os.getenv("secret_value")
    manager = secret_manager.SecretManagerBuilder().build(client_type)
    secret_value = manager.maybe_secret(project_id, secret_id)
    assert secret_value == expected_secret_value


def test_access_gcp_secret_not_exists():
    """Test maybeSecret with a secret not exists."""
    client_type = "gcp"
    secret_id = "db_test_user_not_exists"
    manager = secret_manager.SecretManagerBuilder().build(client_type)
    secret_value = manager.maybe_secret(project_id, secret_id)
    assert secret_value == secret_id

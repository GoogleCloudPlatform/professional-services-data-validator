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

"""Configuration for test sessions.

This is a configuration file for use with `nox <https://nox.thea.codes/>`__.

This configuration is modelled after the one for the `google-cloud-biguery
<https://github.com/googleapis/python-bigquery/blob/master/noxfile.py>`__
package.
"""

import os
import random

import nox

# Python version used for linting.
DEFAULT_PYTHON_VERSION = "3.10"

# Python versions used for testing.
PYTHON_VERSIONS = ["3.8", "3.9", "3.10"]

BLACK_PATHS = (
    "data_validation",
    "samples",
    "tests",
    "third_party",
    "noxfile.py",
    "setup.py",
)
LINT_PACKAGES = ["flake8", "black==22.3.0"]
UNIT_PACKAGES = ["pyfakefs==4.6.2", "freezegun"]


def _setup_session_requirements(session, extra_packages=[]):
    """Install requirements for nox tests."""

    session.install("--upgrade", "pip", "pytest", "pytest-cov", "wheel")
    session.install("--no-cache-dir", "-e", ".")

    if extra_packages:
        session.install(*extra_packages)


@nox.session(python=PYTHON_VERSIONS, venv_backend="venv")
def unit(session):
    # Install all test dependencies, then install local packages in-place.
    _setup_session_requirements(session, extra_packages=UNIT_PACKAGES)

    # Run py.test against the unit tests.
    session.run(
        "py.test",
        "--quiet",
        "--cov=data_validation",
        "--cov=tests.unit",
        "--cov-append",
        "--cov-config=.coveragerc",
        "--cov-report=term",
        os.path.join("tests", "unit"),
        env={"PSO_DV_CONN_HOME": ""},
        *session.posargs,
    )


@nox.session(venv_backend="venv")
def unit_small(session):
    unit(session)


@nox.session(python=PYTHON_VERSIONS, venv_backend="venv")
def samples(session):
    """Run the snippets test suite."""

    # Sanity check: Only run snippets tests if the environment variable is set.
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""):
        session.skip("Credentials must be set via environment variable.")

    # Install all test dependencies, then install local packages in place.
    _setup_session_requirements(session)

    # Run pytest against the samples tests.
    session.run("pytest", "samples", *session.posargs)


@nox.session(python=DEFAULT_PYTHON_VERSION, venv_backend="venv")
def lint(session):
    """Run linters.
    Returns a failure if the linters find linting errors or sufficiently
    serious code quality issues.
    """

    _setup_session_requirements(session, extra_packages=LINT_PACKAGES)

    session.install("--upgrade", "pip", "wheel")
    session.run("flake8", "data_validation")
    session.run("flake8", "tests")
    session.run("black", "--check", *BLACK_PATHS)
    session.run("python", "setup.py", "check", "--strict")


@nox.session(python=DEFAULT_PYTHON_VERSION, venv_backend="venv")
def blacken(session):
    """Run black.
    Format code to uniform standard.
    """
    # Pin a specific version of black, so that the linter doesn't conflict with
    # contributors.

    _setup_session_requirements(session, extra_packages=LINT_PACKAGES)
    session.run("black", *BLACK_PATHS)


@nox.session(python=random.choice(PYTHON_VERSIONS), venv_backend="venv")
def integration_mysql(session):
    """Run MySQL integration tests.
    Ensure MySQL validation is running as expected.
    """
    # Pin a specific version of black, so that the linter doesn't conflict with
    # contributors.
    _setup_session_requirements(session, extra_packages=[])

    test_path = "tests/system/data_sources/test_mysql.py"
    expected_env_vars = ["PROJECT_ID", "MYSQL_PASSWORD", "CLOUD_SQL_CONNECTION"]
    for env_var in expected_env_vars:
        if not os.environ.get(env_var, ""):
            raise Exception("Expected Env Var: %s" % env_var)

    session.run("pytest", test_path, *session.posargs)


@nox.session(python=random.choice(PYTHON_VERSIONS), venv_backend="venv")
def integration_postgres(session):
    """Run Postgres integration tests.
    Ensure Postgres validation is running as expected.
    """
    # Pin a specific version of black, so that the linter doesn't conflict with
    # contributors.
    _setup_session_requirements(session, extra_packages=[])

    test_path = "tests/system/data_sources/test_postgres.py"
    expected_env_vars = ["PROJECT_ID", "POSTGRES_PASSWORD", "CLOUD_SQL_CONNECTION"]
    for env_var in expected_env_vars:
        if not os.environ.get(env_var, ""):
            raise Exception("Expected Env Var: %s" % env_var)

    session.run("pytest", test_path, *session.posargs)


@nox.session(python=random.choice(PYTHON_VERSIONS), venv_backend="venv")
def integration_sql_server(session):
    """Run SQL Server integration tests.
    Ensure SQL Server validation is running as expected.
    """
    # Pin a specific version of black, so that the linter doesn't conflict with
    # contributors.
    _setup_session_requirements(session, extra_packages=["pyodbc"])

    test_path = "tests/system/data_sources/test_sql_server.py"
    expected_env_vars = ["PROJECT_ID", "SQL_SERVER_PASSWORD", "CLOUD_SQL_CONNECTION"]
    for env_var in expected_env_vars:
        if not os.environ.get(env_var, ""):
            raise Exception("Expected Env Var: %s" % env_var)

    session.run("pytest", test_path, *session.posargs)


@nox.session(python=random.choice(PYTHON_VERSIONS), venv_backend="venv")
def integration_bigquery(session):
    """Run BigQuery integration tests.
    Ensure BigQuery validation is running as expected.
    """
    _setup_session_requirements(session, extra_packages=[])

    test_path = "tests/system/data_sources/test_bigquery.py"
    env_vars = {"PROJECT_ID": os.environ.get("PROJECT_ID", "pso-kokoro-resources")}
    for env_var in env_vars:
        if not env_vars[env_var]:
            raise Exception("Expected Env Var: %s" % env_var)

    session.run("pytest", test_path, env=env_vars, *session.posargs)


@nox.session(python=random.choice(PYTHON_VERSIONS), venv_backend="venv")
def integration_spanner(session):
    """Run Spanner integration tests.
    Ensure Spanner validation is running as expected.
    """
    _setup_session_requirements(session, extra_packages=[])

    expected_env_vars = ["PROJECT_ID"]
    for env_var in expected_env_vars:
        if not os.environ.get(env_var, ""):
            raise Exception("Expected Env Var: %s" % env_var)

    session.run("pytest", "tests/system/data_sources/test_spanner.py", *session.posargs)


@nox.session(python=random.choice(PYTHON_VERSIONS), venv_backend="venv")
def integration_teradata(session):
    """Run Teradata integration tests.
    Ensure Teradata validation is running as expected.
    """
    _setup_session_requirements(session, extra_packages=["teradatasql"])

    expected_env_vars = ["PROJECT_ID", "TERADATA_PASSWORD", "TERADATA_HOST"]
    for env_var in expected_env_vars:
        if not os.environ.get(env_var, ""):
            raise Exception("Expected Env Var: %s" % env_var)

    session.run(
        "pytest", "tests/system/data_sources/test_teradata.py", *session.posargs
    )


@nox.session(python=random.choice(PYTHON_VERSIONS), venv_backend="venv")
def integration_state(session):
    """Run StateManager integration tests.
    Ensure the StateManager is running as expected.
    """
    _setup_session_requirements(session, extra_packages=[])

    test_path = "tests/system/test_state_manager.py"
    session.run("pytest", test_path, *session.posargs)


@nox.session(python=random.choice(PYTHON_VERSIONS), venv_backend="venv")
def integration_oracle(session):
    """Run Oracle integration tests.
    Ensure Oracle validation is running as expected.
    """
    _setup_session_requirements(session, extra_packages=["cx_Oracle"])

    expected_env_vars = [
        "PROJECT_ID",
        "ORACLE_PASSWORD",
        "ORACLE_HOST",
        "POSTGRES_PASSWORD",
        "CLOUD_SQL_CONNECTION",
    ]
    for env_var in expected_env_vars:
        if not os.environ.get(env_var, ""):
            raise Exception("Expected Env Var: %s" % env_var)

    session.run("pytest", "tests/system/data_sources/test_oracle.py", *session.posargs)


@nox.session(python=random.choice(PYTHON_VERSIONS), venv_backend="venv")
def integration_hive(session):
    """Run Hive integration tests.
    Ensure Hive validation is running as expected.
    """
    _setup_session_requirements(session, extra_packages=["PyHive", "hdfs"])

    expected_env_vars = ["PROJECT_ID", "HIVE_HOST"]
    for env_var in expected_env_vars:
        if not os.environ.get(env_var, ""):
            raise Exception("Expected Env Var: %s" % env_var)

    session.run("pytest", "tests/system/data_sources/test_hive.py", *session.posargs)


@nox.session(python=random.choice(PYTHON_VERSIONS), venv_backend="venv")
def integration_snowflake(session):
    """Run Snowflake integration tests.
    Ensure Snowflake validation is running as expected.
    """
    _setup_session_requirements(
        session, extra_packages=["snowflake-sqlalchemy", "snowflake-connector-python"]
    )

    expected_env_vars = [
        "PROJECT_ID",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
    ]
    for env_var in expected_env_vars:
        if not os.environ.get(env_var, ""):
            raise Exception("Expected Env Var: %s" % env_var)

    session.run(
        "pytest", "tests/system/data_sources/test_snowflake.py", *session.posargs
    )


@nox.session(python=random.choice(PYTHON_VERSIONS), venv_backend="venv")
def integration_db2(session):
    """Run DB2 integration tests.
    Ensure DB2 validation is running as expected.
    """
    _setup_session_requirements(session, extra_packages=["ibm-db-sa"])

    expected_env_vars = [
        "PROJECT_ID",
        "DB2_HOST",
        "DB2_PASSWORD",
    ]
    for env_var in expected_env_vars:
        if not os.environ.get(env_var, ""):
            raise Exception("Expected Env Var: %s" % env_var)

    session.run("pytest", "tests/system/data_sources/test_db2.py", *session.posargs)


@nox.session(python=random.choice(PYTHON_VERSIONS), venv_backend="venv")
def integration_secrets(session):
    """
    Run SecretManager integration tests.
    Ensure the SecretManager is running as expected.
    """
    _setup_session_requirements(session, extra_packages=[])

    expected_env_vars = ["PROJECT_ID"]
    for env_var in expected_env_vars:
        if not os.environ.get(env_var, ""):
            raise Exception("Expected Env Var: %s" % env_var)

    test_path = "tests/system/test_secret_manager.py"
    session.run("pytest", test_path, *session.posargs)


@nox.session(python=random.choice(PYTHON_VERSIONS), venv_backend="venv")
def integration_filesystem(session):
    """Run Filesystem integration tests.
    Ensure Filesystem validation is running as expected.
    """
    _setup_session_requirements(session, extra_packages=["gcsfs"])

    test_path = "tests/system/data_sources/test_filesystem.py"
    env_vars = {"PROJECT_ID": os.environ.get("PROJECT_ID", "pso-kokoro-resources")}
    for env_var in env_vars:
        if not env_vars[env_var]:
            raise Exception("Expected Env Var: %s" % env_var)

    session.run("pytest", test_path, env=env_vars, *session.posargs)

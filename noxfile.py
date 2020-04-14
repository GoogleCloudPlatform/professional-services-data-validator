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

import nox


PYTHON_VERSION = "3.6"
BLACK_PATHS = ("data_validation", "samples", "tests", "noxfile.py", "setup.py")


def _setup_session_requirements(session, extra_packages=[]):
    """Install requirements for nox tests."""

    session.install("--upgrade", "pip", "pytest", "pytest-cov", "wheel")
    session.install("-e", ".")

    if extra_packages:
        session.install(*extra_packages)


@nox.session(python=PYTHON_VERSION, venv_backend="venv")
def unit(session):
    # Install all test dependencies, then install local packages in-place.
    _setup_session_requirements(session)

    # Run py.test against the unit tests.
    session.run(
        "py.test",
        "--quiet",
        "--cov=data_validation",
        "--cov=tests.unit",
        "--cov-append",
        "--cov-config=.coveragerc",
        "--cov-report=",
        os.path.join("tests", "unit"),
        *session.posargs,
    )


@nox.session(python=PYTHON_VERSION, venv_backend="venv")
def samples(session):
    """Run the snippets test suite."""

    # Sanity check: Only run snippets tests if the environment variable is set.
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""):
        session.skip("Credentials must be set via environment variable.")

    # Install all test dependencies, then install local packages in place.
    _setup_session_requirements(session)

    # Run pytest against the samples tests.
    session.run("pytest", "samples", *session.posargs)


@nox.session(python=PYTHON_VERSION, venv_backend="venv")
def lint(session):
    """Run linters.
    Returns a failure if the linters find linting errors or sufficiently
    serious code quality issues.
    """

    _setup_session_requirements(session, extra_packages=["flake8", "black==19.10b0"])
    session.run("flake8", "data_validation")
    session.run("flake8", "tests")
    session.run("black", "--check", *BLACK_PATHS)


@nox.session(python=PYTHON_VERSION, venv_backend="venv")
def lint_setup_py(session):
    """Verify that setup.py is valid."""

    session.install("--upgrade", "pip", "wheel")
    session.run("python", "setup.py", "check", "--strict")


@nox.session(python=PYTHON_VERSION, venv_backend="venv")
def blacken(session):
    """Run black.
    Format code to uniform standard.
    """
    # Pin a specific version of black, so that the linter doesn't conflict with
    # contributors.
    _setup_session_requirements(session, extra_packages=["black==19.10b0"])
    session.run("black", *BLACK_PATHS)


@nox.session(python=PYTHON_VERSION, venv_backend="venv")
def integration_mysql(session):
    """Run MySQL integration tests.
    Ensure MySQL validation is running as expected.
    """
    # Pin a specific version of black, so that the linter doesn't conflict with
    # contributors.
    _setup_session_requirements(session, extra_packages=["black==19.10b0"])

    test_path = "tests/system/data_sources/test_mysql.py"
    expected_env_vars = ["MYSQL_HOST", "MYSQL_PASSWORD"]
    for env_var in expected_env_vars:
        if not os.environ.get(env_var, ""):
            raise Exception("Expected Env Var: %s" % env_var)

    session.run("pytest", test_path, *session.posargs)


@nox.session(python=PYTHON_VERSION, venv_backend="venv")
def integration_bigquery(session):
    """Run BigQuery integration tests.
    Ensure BigQuery validation is running as expected.
    """
    _setup_session_requirements(session, extra_packages=["black==19.10b0"])

    test_path = "tests/system/data_sources/test_bigquery.py"
    expected_env_vars = []
    for env_var in expected_env_vars:
        if not os.environ.get(env_var, ""):
            raise Exception("Expected Env Var: %s" % env_var)

    session.run("pytest", test_path, *session.posargs)

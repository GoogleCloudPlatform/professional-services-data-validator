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

try:
    import urllib3.contrib.pyopenssl
    
    # Store original methods/properties
    orig_verify_mode_fset = urllib3.contrib.pyopenssl.PyOpenSSLContext.verify_mode.fset
    orig_options_fset = urllib3.contrib.pyopenssl.PyOpenSSLContext.options.fset
    orig_verify_flags_fset = urllib3.contrib.pyopenssl.PyOpenSSLContext.verify_flags.fset
    orig_load_verify_locations = urllib3.contrib.pyopenssl.PyOpenSSLContext.load_verify_locations

    def patched_verify_mode_setter(self, value):
        if getattr(self, "_dvt_verify_mode", None) == value:
            return
        try:
            orig_verify_mode_fset(self, value)
            self._dvt_verify_mode = value
        except ValueError as e:
            if "Context has already been used" in str(e):
                return
            raise

    def patched_options_setter(self, value):
        if getattr(self, "_dvt_options", None) == value:
            return
        try:
            orig_options_fset(self, value)
            self._dvt_options = value
        except ValueError as e:
            if "Context has already been used" in str(e):
                return
            raise

    def patched_verify_flags_setter(self, value):
        if getattr(self, "_dvt_verify_flags", None) == value:
            return
        try:
            orig_verify_flags_fset(self, value)
            self._dvt_verify_flags = value
        except ValueError as e:
            if "Context has already been used" in str(e):
                return
            raise

    def patched_load_verify_locations(self, cafile=None, capath=None, cadata=None):
        cache_key = (cafile, capath, cadata)
        if getattr(self, "_dvt_loaded_locations", None) is None:
            self._dvt_loaded_locations = set()
        if cache_key in self._dvt_loaded_locations:
            return
        try:
            orig_load_verify_locations(self, cafile, capath, cadata)
            self._dvt_loaded_locations.add(cache_key)
        except ValueError as e:
            if "Context has already been used" in str(e):
                return
            raise

    orig_set_alpn_protocols = urllib3.contrib.pyopenssl.PyOpenSSLContext.set_alpn_protocols

    def patched_set_alpn_protocols(self, protocols):
        cache_key = tuple(protocols)
        if getattr(self, "_dvt_alpn_protocols", None) == cache_key:
            return
        try:
            orig_set_alpn_protocols(self, protocols)
            self._dvt_alpn_protocols = cache_key
        except ValueError as e:
            if "Context has already been used" in str(e):
                return
            raise

    # Re-apply patched properties and methods
    urllib3.contrib.pyopenssl.PyOpenSSLContext.verify_mode = property(
        urllib3.contrib.pyopenssl.PyOpenSSLContext.verify_mode.fget,
        patched_verify_mode_setter
    )
    urllib3.contrib.pyopenssl.PyOpenSSLContext.options = property(
        urllib3.contrib.pyopenssl.PyOpenSSLContext.options.fget,
        patched_options_setter
    )
    urllib3.contrib.pyopenssl.PyOpenSSLContext.verify_flags = property(
        urllib3.contrib.pyopenssl.PyOpenSSLContext.verify_flags.fget,
        patched_verify_flags_setter
    )
    urllib3.contrib.pyopenssl.PyOpenSSLContext.load_verify_locations = patched_load_verify_locations
    urllib3.contrib.pyopenssl.PyOpenSSLContext.set_alpn_protocols = patched_set_alpn_protocols

except Exception:
    pass

import datetime
import os
import random

import google.cloud.bigquery
import pytest

ALPHABET = "abcdefghijklmnopqrstuvwxyz"


def pytest_addoption(parser):
    parser.addoption("--no-cloud-sql", action="store_const", const=True)
    parser.addoption(
        "--run-slow", action="store_true", default=False, help="Include slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-slow"):
        # --run-slow given in CLI: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="Need --run-slow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture(scope="module")
def bigquery_client():
    project_id = os.environ["PROJECT_ID"]

    return google.cloud.bigquery.Client(project=project_id)


@pytest.fixture(scope="module")
def bigquery_dataset_id(bigquery_client):
    now = datetime.datetime.now()
    project_id = os.environ["PROJECT_ID"]
    dataset_id = (
        f"{project_id}.data_validator_tests_"
        + now.strftime("%Y%m%d%H%M")
        + random.choice(ALPHABET)
        + random.choice(ALPHABET)
        + random.choice(ALPHABET)
        + random.choice(ALPHABET)
        + random.choice(ALPHABET)
        + random.choice(ALPHABET)
    )
    bigquery_client.create_dataset(dataset_id)
    yield dataset_id
    bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

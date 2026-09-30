# Copyright 2026 Google LLC
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

import pytest

from data_validation import gcs_helper


@pytest.fixture(autouse=True)
def clear_storage_client_cache():
    """Ensure cached storage.Client instances or mocks do not leak across unit tests."""
    gcs_helper._get_storage_client.cache_clear()
    yield
    gcs_helper._get_storage_client.cache_clear()

# Copyright 2025 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from third_party.ibis.ibis_spanner_postgres import Backend as SpannerPostgresBackend


def spanner_postgres_connect(
    host=None,
    port=None,
    user="u",
    password="p",
):
    """TODO: add docstring."""
    backend = SpannerPostgresBackend()
    backend.do_connect(
        host=host,
        port=port,
        user=user,
        password=password,
    )
    return backend

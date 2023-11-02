# Copyright 2021 Google Inc.
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

import google.cloud.spanner  # noqa: F401, fail early if spanner is missing

from third_party.ibis.ibis_cloud_spanner import Backend as SpannerBackend


def spanner_connect(
    instance_id,
    database_id,
    project_id=None,
    credentials=None,
):
    """Create a Cloud Spanner Backend for use with Ibis.

    Parameters
    ----------
    instance_id : str
        A Cloud Spanner Instance id.
    database_id : str
        A database id inside of the Cloud Spanner Instance
    project_id  : str (Optional)
        The ID of the project which owns the instances, tables and data.
    """
    backend = SpannerBackend()
    backend.do_connect(
        instance_id=instance_id,
        database_id=database_id,
        project_id=project_id,
        credentials=credentials,
    )
    return backend

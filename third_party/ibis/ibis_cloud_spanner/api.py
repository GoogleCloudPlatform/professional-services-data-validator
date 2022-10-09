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


"""CloudSpanner public API."""


from third_party.ibis.ibis_cloud_spanner.client import Backend

import google.cloud.spanner  # noqa: F401, fail early if spanner is missing
import ibis.common.exceptions as com

__all__ = ("compile", "connect", "verify")


def compile(expr, params=None, **kwargs):
    """Compile an expression for Cloud Spanner.
    Returns
    -------
    compiled : str
    See Also
    --------
    ibis.expr.types.Expr.compile
    """
    backend = Backend()
    return backend.compile(expr, params=params, **kwargs)


def verify(expr, params=None):
    """Check if an expression can be compiled using Cloud Spanner."""
    try:
        compile(expr, params=params)
        return True
    except com.TranslationError:
        return False


def connect(
    instance_id,
    database_id,
    project_id=None,
) -> Backend:
    """Create a CloudSpanner Backend for use with Ibis.

    Parameters
    ----------
    instance_id : str
        A Cloud Spanner Instance id.
    database_id : str
        A database id inside of the Cloud Spanner Instance
    project_id  : str (Optional)
        The ID of the project which owns the instances, tables and data.

    Returns
    -------
    Backend

    """

    return Backend.connect(
        instance_id=instance_id,
        database_id=database_id,
        project_id=project_id,
    )

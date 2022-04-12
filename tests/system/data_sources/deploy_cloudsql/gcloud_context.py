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

""" Context manager for gcloud which redirects config directory to temp location """

import os
import subprocess


class GCloudContext(object):
    def __init__(self, project_id, creds_file=None):
        self._project_id = project_id
        self._creds_file = creds_file

    def __enter__(self):
        if self._creds_file:
            self.Run("auth", "activate-service-account", "--key-file", self._creds_file)
        self.Run("config", "set", "project", self._project_id)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass

    def Run(self, *args, **kwargs):
        """Runs gcloud command and returns output"""
        env = kwargs.pop("env", None)
        if not env:
            env = os.environ.copy()
        env["CLOUDSDK_CORE_PRINT_UNHANDLED_TRACEBACKS"] = "true"
        fullcmd = ("gcloud",) + args
        return subprocess.check_output(fullcmd, env=env, **kwargs)

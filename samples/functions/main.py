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

import os
import pandas
import json
from yaml import dump, load, Dumper, Loader

from data_validation.data_validations import DataValidation


def _clean_dataframe(df):
    rows = df.to_dict(orient="record")
    for row in rows:
        for key in row:
            if type(row[key]) in [pandas.Timestamp]:
                row[key] = str(row[key])

    return json.dumps(rows)


def main(request):
    """Handle incoming Data Validation requests.

    request (flask.Request): HTTP request object.
    """
    try:
        config = request.json["config"]
        validator = DataValidation(config)
        df = validator.execute()

        return _clean_dataframe(df)
    except Exception as e:
        return "Unknown Error: {}".format(e)

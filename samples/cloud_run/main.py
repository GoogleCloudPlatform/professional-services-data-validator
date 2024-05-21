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

import json
import os
from data_validation import data_validation
import flask
import pandas

app = flask.Flask(__name__)


def _clean_dataframe(df):
    rows = df.to_dict(orient="record")
    for row in rows:
        for key in row:
            if type(row[key]) in [pandas.Timestamp]:
                row[key] = str(row[key])

    return json.dumps(rows)


def _get_request_content(request):
    return request.json


def validate(config):
    """Run Data Validation against the supplied config."""
    validator = data_validation.DataValidation(config)
    df = validator.execute()

    return _clean_dataframe(df)


def main(request):
    """Handle incoming Data Validation requests.

    request (flask.Request): HTTP request object.
    """
    try:
        config = _get_request_content(request)["config"]
        return validate(config)
    except Exception as e:
        return "Unknown Error: {}".format(e)


@app.route("/", methods=["POST"])
def run():
    try:
        config = _get_request_content(flask.request)
        result = validate(config)
        return str(result)
    except Exception as e:
        print(e)
        return "Found Error: {}".format(e)


@app.route("/test", methods=["POST"])
def other():
    return _get_request_content(flask.request)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

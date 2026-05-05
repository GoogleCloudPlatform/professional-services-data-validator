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

import logging
import os
from data_validation import data_validation
import flask

app = flask.Flask(__name__)


def _clean_dataframe(df):
    return df.to_json(orient="records", date_format="iso")


def _get_request_content(request):
    return request.json


def validate(config):
    """Run Data Validation against the supplied config."""
    validator = data_validation.DataValidation(config)
    df = validator.execute()

    return _clean_dataframe(df)


@app.route("/", methods=["POST"])
def run():
    """Handle incoming Data Validation requests.

    request (flask.Request): HTTP request object.
    """
    try:
        config = _get_request_content(flask.request)
        result = validate(config)
        return flask.Response(result, mimetype="application/json")
    except Exception:
        logging.exception("An error occurred during validation")
        return flask.Response(
            "An internal server error occurred.", status=500, mimetype="text/plain"
        )


@app.route("/test", methods=["POST"])
def other():
    return _get_request_content(flask.request)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

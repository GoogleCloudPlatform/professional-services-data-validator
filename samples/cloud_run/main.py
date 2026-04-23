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

import argparse
import json
import os
from data_validation import data_validation
from data_validation.__main__ import (
    build_config_managers_from_args,
    convert_config_to_json,
)
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


@app.route("/generate_column_config", methods=["POST"])
def generate_column_config():
    try:
        payload = _get_request_content(flask.request)
        from data_validation.cli_tools import _configure_column_parser

        dummy_parser = argparse.ArgumentParser()
        _configure_column_parser(dummy_parser)

        # Dynamically pull all defaults from registered actions
        defaults = {
            action.dest: action.default
            for action in dummy_parser._actions
            if action.dest != "help"
        }
        # Inject the sub-command routing defaults
        defaults["command"] = "validate"
        defaults["validate_cmd"] = "column"
        defaults["verbose"] = False
        defaults["log_level"] = "INFO"
        defaults.update(payload)
        args = argparse.Namespace(**defaults)

        config_managers = build_config_managers_from_args(args)
        json_config = convert_config_to_json(config_managers)
        return flask.Response(json.dumps(json_config), mimetype="application/json")
    except ValueError as ve:
        return flask.Response(f"Bad Request: {ve}", status=400, mimetype="text/plain")
    except Exception as e:
        logging.exception("An error occurred during configuration. generation")
        return flask.Response(
            "An internal server error occurred.", status=500, mimetype="text/plain"
        )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

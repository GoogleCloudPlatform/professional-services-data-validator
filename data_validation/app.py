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
import logging
import json
import os
from data_validation import data_validation, state_manager
from data_validation.__main__ import (
    build_config_managers_from_args,
    convert_config_to_json,
    store_yaml_config_file,
)
import flask

app = flask.Flask(__name__)


def _clean_dataframe(df):
    return df.to_json(orient="records", date_format="iso")


def _get_request_content(request):
    return request.json


def _get_args_from_payload(payload, parser):
    # Dynamically pull all defaults from registered actions
    defaults = {
        action.dest: action.default
        for action in parser._actions
        if action.dest != "help"
    }
    # Map hyphens to underscores in payload for argparse compatibility
    processed_payload = {k.replace("-", "_"): v for k, v in payload.items()}
    defaults.update(processed_payload)
    return argparse.Namespace(**defaults)


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
        logging.exception(e)
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

        args = _get_args_from_payload(payload, dummy_parser)
        # Inject the sub-command routing defaults
        args.command = "validate"
        args.validate_cmd = "column"
        if not hasattr(args, "verbose"):
            args.verbose = False
        if not hasattr(args, "log_level"):
            args.log_level = "INFO"

        if not getattr(args, "config_file", None):
            return flask.Response(
                "Bad Request: config_file is a mandatory parameter",
                status=400,
                mimetype="text/plain",
            )

        config_managers = build_config_managers_from_args(args)

        store_yaml_config_file(args, config_managers)
        return flask.Response(
            f"Success! Config output written to {args.config_file}",
            mimetype="text/plain",
        )
    except ValueError as ve:
        return flask.Response(f"Bad Request: {ve}", status=400, mimetype="text/plain")
    except Exception as e:
        logging.exception("An error occurred during column configuration generation")
        return flask.Response(
            f"An internal server error occurred: {e}",
            status=500,
            mimetype="text/plain",
        )


@app.route("/generate_row_config", methods=["POST"])
def generate_row_config():
    try:
        payload = _get_request_content(flask.request)
        from data_validation.cli_tools import _configure_row_parser

        dummy_parser = argparse.ArgumentParser()
        optional_arguments = dummy_parser.add_argument_group("optional arguments")
        required_arguments = dummy_parser.add_argument_group("required arguments")
        _configure_row_parser(dummy_parser, optional_arguments, required_arguments)

        args = _get_args_from_payload(payload, dummy_parser)
        # Inject the sub-command routing defaults
        args.command = "validate"
        args.validate_cmd = "row"
        if not hasattr(args, "verbose"):
            args.verbose = False
        if not hasattr(args, "log_level"):
            args.log_level = "INFO"

        if not getattr(args, "config_file", None):
            return flask.Response(
                "Bad Request: config_file is a mandatory parameter",
                status=400,
                mimetype="text/plain",
            )

        config_managers = build_config_managers_from_args(args)

        store_yaml_config_file(args, config_managers)
        return flask.Response(
            f"Success! Config output written to {args.config_file}",
            mimetype="text/plain",
        )
    except ValueError as ve:
        return flask.Response(f"Bad Request: {ve}", status=400, mimetype="text/plain")
    except Exception as e:
        logging.exception("An error occurred during row configuration generation")
        return flask.Response(
            f"An internal server error occurred: {e}",
            status=500,
            mimetype="text/plain",
        )


@app.route("/find_tables", methods=["POST"])
def find_tables():
    try:
        payload = _get_request_content(flask.request)
        from data_validation.cli_tools import _configure_find_tables
        from data_validation.find_tables import find_tables_using_string_matching

        dummy_parser = argparse.ArgumentParser()
        subparsers = dummy_parser.add_subparsers()
        _configure_find_tables(subparsers)
        find_tables_parser = subparsers.choices["find-tables"]

        args = _get_args_from_payload(payload, find_tables_parser)
        # Inject the sub-command routing defaults
        args.command = "find-tables"

        result = find_tables_using_string_matching(args)
        return flask.Response(result, mimetype="application/json")
    except Exception as e:
        logging.exception("An error occurred during find_tables")
        return flask.Response(
            f"An internal server error occurred: {e}",
            status=500,
            mimetype="text/plain",
        )


@app.route("/get_connections", methods=["POST"])
def get_connections():
    try:
        mgr = state_manager.StateManager()
        connections = mgr.list_connections()
        return flask.Response(json.dumps(connections), mimetype="application/json")
    except Exception:
        logging.exception("An error occurred while listing connections")
        return flask.Response(
            "An internal server error occurred.", status=500, mimetype="text/plain"
        )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

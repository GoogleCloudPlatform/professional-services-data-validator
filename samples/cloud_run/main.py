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

import flask

from data_validation import data_validation, state_manager
from data_validation.__main__ import (
    build_config_managers_from_args,
    store_yaml_config_file,
    store_config_dir,
)

app = flask.Flask(__name__)


def _clean_dataframe(df):
    return df.to_json(orient="records", date_format="iso")


def _get_request_content(request):
    return request.json


def _get_args_from_payload(payload, parser, **kwargs):
    # Dynamically pull all defaults from registered actions
    defaults = {
        action.dest: action.default
        for action in parser._actions
        if action.dest != "help"
    }
    # Map hyphens to underscores in payload for argparse compatibility
    processed_payload = {k.replace("-", "_"): v for k, v in payload.items()}
    defaults.update(processed_payload)
    args = argparse.Namespace(**defaults)

    for key, value in kwargs.items():
        setattr(args, key, value)

    if not hasattr(args, "verbose"):
        args.verbose = False
    if not hasattr(args, "log_level"):
        args.log_level = "INFO"

    return args


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


@app.route("/ping", methods=["GET", "POST"])
def ping():
    """Lightweight health check endpoint for load balancers and uptime probes."""
    return flask.Response("OK", status=200, mimetype="text/plain")


@app.route("/version", methods=["GET", "POST"])
def version():
    """Returns the running Data Validation Tool version."""
    from data_validation import __version__

    return flask.Response(
        json.dumps({"version": __version__}), status=200, mimetype="application/json"
    )


@app.route("/generate_column_config", methods=["POST"])
def generate_column_config():
    try:
        payload = _get_request_content(flask.request)
        from data_validation.cli_tools import _configure_column_parser

        dummy_parser = argparse.ArgumentParser()
        _configure_column_parser(dummy_parser)

        args = _get_args_from_payload(
            payload, dummy_parser, command="validate", validate_cmd="column"
        )

        if not getattr(args, "config_file", None) and not getattr(
            args, "config_dir", None
        ):
            return flask.Response(
                "Bad Request: either config_file or config_dir is a mandatory parameter",
                status=400,
                mimetype="text/plain",
            )

        if not getattr(args, "result_handler", None):
            return flask.Response(
                "Bad Request: result_handler is a mandatory parameter",
                status=400,
                mimetype="text/plain",
            )

        config_managers = build_config_managers_from_args(args)

        if args.config_file:
            store_yaml_config_file(args, config_managers)
        elif getattr(args, "config_dir", None):
            store_config_dir(args, config_managers, is_json=False)
        target_path = getattr(args, "config_file", None) or getattr(
            args, "config_dir", None
        )

        return flask.Response(
            f"Success! Config output written to {target_path}",
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

        args = _get_args_from_payload(
            payload, dummy_parser, command="validate", validate_cmd="row"
        )

        if not getattr(args, "config_file", None) and not getattr(
            args, "config_dir", None
        ):
            return flask.Response(
                "Bad Request: either config_file or config_dir is a mandatory parameter",
                status=400,
                mimetype="text/plain",
            )

        if not getattr(args, "result_handler", None):
            return flask.Response(
                "Bad Request: result_handler is a mandatory parameter",
                status=400,
                mimetype="text/plain",
            )

        config_managers = build_config_managers_from_args(args)

        if args.config_file:
            store_yaml_config_file(args, config_managers)
        elif getattr(args, "config_dir", None):
            store_config_dir(args, config_managers, is_json=False)
        target_path = getattr(args, "config_file", None) or getattr(
            args, "config_dir", None
        )
        return flask.Response(
            f"Success! Config output written to {target_path}",
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


@app.route("/generate_custom_query_row_config", methods=["POST"])
def generate_custom_query_row_config():
    try:
        payload = _get_request_content(flask.request)
        from data_validation.cli_tools import _configure_custom_query_row_parser

        dummy_parser = argparse.ArgumentParser()
        _configure_custom_query_row_parser(dummy_parser)

        args = _get_args_from_payload(
            payload,
            dummy_parser,
            command="validate",
            validate_cmd="custom-query",
            custom_query_type="row",
        )

        if not getattr(args, "config_file", None):
            return flask.Response(
                "Bad Request: config_file is a mandatory parameter",
                status=400,
                mimetype="text/plain",
            )

        if not getattr(args, "source_query", None):
            return flask.Response(
                "Bad Request: source_query is a mandatory parameter",
                status=400,
                mimetype="text/plain",
            )

        if not getattr(args, "target_query", None):
            return flask.Response(
                "Bad Request: target_query is a mandatory parameter",
                status=400,
                mimetype="text/plain",
            )

        if not getattr(args, "result_handler", None):
            return flask.Response(
                "Bad Request: result_handler is a mandatory parameter",
                status=400,
                mimetype="text/plain",
            )

        config_managers = build_config_managers_from_args(args)

        if args.config_file:
            store_yaml_config_file(args, config_managers)
        target_path = getattr(args, "config_file", None)
        return flask.Response(
            f"Success! Config output written to {target_path}",
            mimetype="text/plain",
        )
    except ValueError as ve:
        return flask.Response(f"Bad Request: {ve}", status=400, mimetype="text/plain")
    except Exception as e:
        logging.exception(
            "An error occurred during custom query row configuration generation"
        )
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

        args = _get_args_from_payload(
            payload, find_tables_parser, command="find-tables"
        )

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


@app.route("/bulk_table_metadata", methods=["POST"])
def bulk_table_metadata():
    try:
        payload = _get_request_content(flask.request)
        connection_name = payload.get("connection_name")
        tables = payload.get("tables")

        if not connection_name or not isinstance(tables, list):
            return flask.Response(
                "Bad Request: connection_name and a 'tables' list are required parameters",
                status=400,
                mimetype="text/plain",
            )

        mgr = state_manager.StateManager()
        connection_config = mgr.get_connection_config(connection_name)

        from data_validation import clients

        client = clients.get_data_client(connection_config)

        results = []
        for table_spec in tables:
            schema_name = table_spec.get("schema_name")
            table_name = table_spec.get("table_name")

            if not schema_name or not table_name:
                results.append(
                    {
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "error": "Bad Request: schema_name and table_name are required",
                    }
                )
                continue

            try:
                primary_keys = []
                if hasattr(client, "list_primary_key_columns"):
                    primary_keys = client.list_primary_key_columns(
                        schema_name, table_name
                    )

                raw_metadata = []
                if hasattr(client, "raw_column_metadata"):
                    raw_metadata_iter = client.raw_column_metadata(
                        schema_name, table_name
                    )
                    # Compress metadata to include only column name and data type
                    raw_metadata = [(col[0], col[1]) for col in raw_metadata_iter]

                estimated_row_count = None
                if hasattr(client, "estimated_row_count"):
                    estimated_row_count = client.estimated_row_count(
                        schema_name, table_name
                    )

                results.append(
                    {
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "primary_keys": primary_keys,
                        "raw_column_metadata": raw_metadata,
                        "estimated_row_count": estimated_row_count,
                    }
                )
            except Exception as table_exc:
                logging.exception(
                    f"Error fetching metadata for {schema_name}.{table_name}"
                )
                results.append(
                    {
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "error": str(table_exc),
                    }
                )

        return flask.Response(json.dumps(results), mimetype="application/json")
    except Exception as e:
        logging.exception("An error occurred during bulk_table_metadata")
        return flask.Response(
            f"An internal server error occurred: {e}",
            status=500,
            mimetype="text/plain",
        )


@app.route("/oracle_check_dbms_crypto", methods=["POST"])
def oracle_check_dbms_crypto():
    try:
        payload = _get_request_content(flask.request)
        connection_name = payload.get("connection_name")

        if not connection_name:
            return flask.Response(
                "Bad Request: connection_name is a required parameter",
                status=400,
                mimetype="text/plain",
            )

        mgr = state_manager.StateManager()
        connection_config = mgr.get_connection_config(connection_name)

        from data_validation import clients

        with clients.get_data_client_ctx(connection_config) as client:
            sql = "SELECT DBMS_CRYPTO.HASH(TO_CLOB('DVT'),4) FROM dual"
            cursor = client.raw_sql(sql)
            cursor.fetchone()
            try:
                cursor.close()
            except Exception:
                pass

        return flask.Response("OK", status=200, mimetype="text/plain")
    except Exception as e:
        logging.exception("An error occurred during oracle_check_dbms_crypto")
        return flask.Response(
            str(e),
            status=500,
            mimetype="text/plain",
        )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

# Copyright 2026 Google LLC
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
import os
from unittest import mock

import pytest

from data_validation import cli_tools, config_runner, exceptions

CONFIG_RUNNER_ARGS_1 = {
    "verbose": False,
    "log_level": "INFO",
    "command": "configs",
    "validation_config_cmd": "run",
    "dry_run": False,
    "config_file": "gs://pso-project/resources/test/unit/test__main/3validations/first.yaml",
    "config_dir": None,
    "kube_completions": True,
}
CONFIG_RUNNER_ARGS_2 = {
    "verbose": False,
    "log_level": "INFO",
    "dry_run": False,
    "command": "configs",
    "validation_config_cmd": "run",
    "kube_completions": True,
    "config_dir": "/tmp/test/unit/test__main/3validations",
}
CONFIG_RUNNER_ARGS_3 = {
    "verbose": False,
    "log_level": "INFO",
    "dry_run": False,
    "command": "configs",
    "kube_completions": True,
    "validation_config_cmd": "run",
    "config_dir": "gs://pso-project/resources/test/unit/test__main/4partitions",
}
CONFIG_RUNNER_ARGS_4 = {
    "verbose": False,
    "log_level": "INFO",
    "dry_run": False,
    "command": "configs",
    "kube_completions": False,
    "validation_config_cmd": "run",
    "config_dir": "/tmp/test/unit/test__main/4partitions",
}

CONFIG_RUNNER_EXCEPTION_TEXT = (
    "Error '{}' occurred while running config file {}. Skipping it for now."
)


@pytest.fixture(autouse=True)
def clean_completions_env(monkeypatch):
    """Isolate tests from ambient Kubernetes / Cloud Run environment variables."""
    for var in (
        "JOB_COMPLETION_INDEX",
        "JOB_COMPLETION_COUNT",
        "CLOUD_RUN_TASK_INDEX",
        "CLOUD_RUN_TASK_COUNT",
    ):
        monkeypatch.delenv(var, raising=False)


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_1),
)
def test_config_runner_1(mock_args, mock_build, mock_run, caplog):
    """config_runner, runs the validations, so we have to mock run_validations and examine the arguments
    passed to it. Build Config Managers reads the YAML files and builds the validation configs,
    which also includes creating a connection to the database. That is beyond a unit test, so mock
    build_config_managers_from_yaml.
    First test - run validation on a single file - and provide the -kc argument
    Expected result
    1. One config manager created
    2. Warning about inappropriate use of -kc
    Other test cases can be developed.
    """
    caplog.set_level(logging.WARNING)
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)
    # assert warning is seen
    assert caplog.messages == [
        "--kube-completions or -kc specified, which requires a config directory, however a specific config file is provided."
    ]
    # assert that only one config manager object is present
    assert len(mock_run.call_args.args[1]) == 1


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "data_validation.cli_tools.list_validations",
    return_value=["first.yaml", "second.yaml", "third.yaml"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_2),
)
def test_config_runner_2(mock_args, mock_list, mock_build, mock_run, caplog):
    """Second test - run validation on a directory - and provide the -kc argument,
    but not running in a Kubernetes Completion Configuration. Expected result
    1. Multiple (3) config manager created for validation
    2. Warning about inappropriate use of -kc"""
    caplog.set_level(logging.WARNING)
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)
    # assert warning is seen
    assert caplog.messages == [
        "--kube-completions or -kc specified, however not running in Kubernetes Job completion, check your command line."
    ]
    # assert that validation is called thrice, once for each file
    assert mock_run.call_count == 3


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "data_validation.cli_tools.list_validations",
    return_value=["0000.yaml", "0001.yaml", "0002.yaml", "0003.yaml"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_3(
    mock_args, mock_list, mock_build, mock_run, caplog, monkeypatch
):
    """Second test - run validation on a directory - and provide the -kc argument,
    have system believe it is running in a Kubernetes Completion Environment. Expected result
    1. No warnings when JOB_COMPLETION_COUNT matches number of files
    2. run validation called as though config file is provided (config_dir is None)
    3. run validation config file name corresponds to value of JOB_COMPLETION_INDEX
    4. One config manager created for validation
    """
    caplog.set_level(logging.WARNING)
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "2")
    monkeypatch.setenv("JOB_COMPLETION_COUNT", "4")
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)
    # assert no warnings
    assert caplog.messages == []
    # assert that only one config manager and one validation corresponding to JOB_COMPLETION_INDEX is set.
    assert mock_run.call_args.args[0].config_dir is None
    assert os.path.basename(mock_run.call_args.args[0].config_file) == "0002.yaml"
    assert len(mock_run.call_args.args[1]) == 1


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "data_validation.cli_tools.list_validations",
    return_value=["0000.yaml", "0001.yaml", "0002.yaml", "0003.yaml"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_4),
)
def test_config_runner_4(mock_args, mock_list, mock_build, mock_run, caplog):
    """Third test - run validation on a directory with failures in one validation,
        Running in a non Kube completions environment. Expected Result:
    1. All 4 files are validated, even though one of them raises an exception.
    2. Exception from one validation is trapped, file skipped and raised at the end.
    """
    mock_run.side_effect = [10, ValueError("Boom!"), 12, 10]
    caplog.set_level(logging.WARNING)
    args = cli_tools.get_parsed_args()
    caplog.clear()
    with pytest.raises(exceptions.ValidationException) as e_info:
        config_runner.config_runner(args)
    # assert that exception message was output for the failed validation
    # validation is called four times, once for each file
    # After all four files were validated, an exception was raised back to main to return status
    assert caplog.messages[0] == CONFIG_RUNNER_EXCEPTION_TEXT.format(
        "Boom!", "0001.yaml"
    )
    assert caplog.records[0].exc_info is not None
    assert mock_run.call_count == 4
    assert e_info.value.args[0] == "Some of the validations raised an exception"


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "data_validation.cli_tools.list_validations",
    return_value=["e.yaml", "c.yaml", "a.yaml", "d.yaml", "b.yaml"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_sorted_index_selection_kube(
    mock_args, mock_list, mock_build, mock_run, caplog, monkeypatch
):
    """Test sorted file selection in Kubernetes Completion Environment.
    With job_count=5 and job_index=1, and 5 unsorted files:
    Sorted files: a.yaml, b.yaml, c.yaml, d.yaml, e.yaml
    Task 1 should run b.yaml.
    """
    caplog.set_level(logging.INFO)
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "1")
    monkeypatch.setenv("JOB_COMPLETION_COUNT", "5")
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)

    assert "Running validation for index 1: YAML file b.yaml" in caplog.text
    assert mock_run.call_count == 1
    assert mock_run.call_args.args[0].config_dir is None
    assert os.path.basename(mock_run.call_args.args[0].config_file) == "b.yaml"


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "data_validation.cli_tools.list_validations",
    return_value=["e.yaml", "c.yaml", "a.yaml", "d.yaml", "b.yaml"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_sorted_index_selection_cloud_run(
    mock_args, mock_list, mock_build, mock_run, caplog, monkeypatch
):
    """Test sorted file selection using the Cloud Run environment variables.
    With task_count=5 and task_index=3, and 5 unsorted files:
    Sorted files: a.yaml, b.yaml, c.yaml, d.yaml, e.yaml
    Task 3 should run d.yaml.
    """
    caplog.set_level(logging.INFO)
    monkeypatch.setenv("CLOUD_RUN_TASK_INDEX", "3")
    monkeypatch.setenv("CLOUD_RUN_TASK_COUNT", "5")
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)

    assert "Running validation for index 3: YAML file d.yaml" in caplog.text
    assert mock_run.call_count == 1
    assert mock_run.call_args.args[0].config_dir is None
    assert os.path.basename(mock_run.call_args.args[0].config_file) == "d.yaml"


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "data_validation.cli_tools.list_validations",
    return_value=["table_b.yaml", "table_a.yaml"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_missing_job_completion_count_warning(
    mock_args, mock_list, mock_build, mock_run, caplog, monkeypatch
):
    """Test that when JOB_COMPLETION_COUNT is not set, a warning is logged and the
    file corresponding to the index in sorted order is still validated.
    """
    caplog.set_level(logging.WARNING)
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "0")
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)

    assert caplog.messages == [
        "JOB_COMPLETION_COUNT is not set. It should be set to the number of "
        "validation files in the directory (2)."
    ]
    assert mock_run.call_count == 1
    assert os.path.basename(mock_run.call_args.args[0].config_file) == "table_a.yaml"


@pytest.mark.parametrize(
    "index_var,count_var",
    [
        ("JOB_COMPLETION_INDEX", "JOB_COMPLETION_COUNT"),
        ("CLOUD_RUN_TASK_INDEX", "CLOUD_RUN_TASK_COUNT"),
    ],
)
@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "data_validation.cli_tools.list_validations",
    return_value=["e.yaml", "c.yaml", "a.yaml", "d.yaml", "b.yaml"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    side_effect=lambda *a, **kw: argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_partial_validation_warning(
    mock_args,
    mock_list,
    mock_build,
    mock_run,
    caplog,
    monkeypatch,
    index_var,
    count_var,
):
    """Test that when CLOUD_RUN_TASK_COUNT or JOB_COMPLETION_COUNT is less than the
    number of validation files in the directory, a warning about partial validation is logged.
    """
    caplog.set_level(logging.WARNING)
    monkeypatch.setenv(index_var, "1")
    monkeypatch.setenv(count_var, "2")
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)

    assert caplog.messages == [
        "Task count (2) is less than the number of validation files in the "
        "directory (5). The validation is likely to be partial as not all "
        "the validations in the directory will be executed."
    ]
    assert mock_run.call_count == 1
    assert os.path.basename(mock_run.call_args.args[0].config_file) == "b.yaml"


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "data_validation.cli_tools.list_validations",
    return_value=["a.yaml", "b.yaml", "c.yaml"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_index_out_of_bounds_error(
    mock_args, mock_list, mock_build, mock_run, caplog, monkeypatch
):
    """Test that when the job/task index has no corresponding file, an error is logged
    stating too many jobs/tasks have been instantiated and no validation is run.
    """
    caplog.set_level(logging.WARNING)
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "4")
    monkeypatch.setenv("JOB_COMPLETION_COUNT", "5")
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)

    assert caplog.records[0].levelname == "ERROR"
    assert caplog.messages == [
        "No validation file found for index 4 (directory contains 3 "
        "validation files). Too many jobs/tasks have been instantiated."
    ]
    assert mock_run.call_count == 0


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "data_validation.cli_tools.list_validations",
    return_value=[],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_completions_empty_config_dir(
    mock_args, mock_list, mock_build, mock_run, monkeypatch
):
    """Test that an empty config directory is an error and not a successful no-op.

    A misspelt GCS prefix simply matches no objects rather than raising an error,
    therefore each task has to fail rather than report a clean run of no validations.
    """
    monkeypatch.setenv("CLOUD_RUN_TASK_INDEX", "0")
    monkeypatch.setenv("CLOUD_RUN_TASK_COUNT", "4")
    args = cli_tools.get_parsed_args()
    with pytest.raises(ValueError) as e_info:
        config_runner.config_runner(args)

    assert "No validation YAML files found in config directory" in str(e_info.value)
    assert mock_run.call_count == 0


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "data_validation.cli_tools.list_validations",
    return_value=[],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_4),
)
def test_config_runner_empty_config_dir(mock_args, mock_list, mock_build, mock_run):
    """Test that an empty config directory is also an error outside of completions mode."""
    args = cli_tools.get_parsed_args()
    with pytest.raises(ValueError) as e_info:
        config_runner.config_runner(args)

    assert "No validation YAML files found in config directory" in str(e_info.value)
    assert mock_run.call_count == 0


@pytest.mark.parametrize(
    "index_var,neg_value",
    [("JOB_COMPLETION_INDEX", "-1"), ("CLOUD_RUN_TASK_INDEX", "-2")],
)
@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    side_effect=lambda *a, **kw: argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_negative_index(
    mock_args, mock_build, mock_run, monkeypatch, index_var, neg_value
):
    """Test that a negative task index is rejected rather than indexing from the end of all_files."""
    monkeypatch.setenv(index_var, neg_value)
    args = cli_tools.get_parsed_args()
    with pytest.raises(ValueError) as e_info:
        config_runner.config_runner(args)

    assert f"Task index {neg_value} cannot be negative" in str(e_info.value)
    assert mock_run.call_count == 0


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "data_validation.cli_tools.list_validations",
    return_value=["0000.yaml", "0001.yaml", "0002.yaml", "0003.yaml"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_invalid_count(
    mock_args, mock_list, mock_build, mock_run, caplog, monkeypatch
):
    """Test that an invalid non-numeric task count logs warnings and still runs the indexed file."""
    caplog.set_level(logging.WARNING)
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "2")
    monkeypatch.setenv("JOB_COMPLETION_COUNT", "not-a-number")
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)

    assert "Ignoring invalid task count 'not-a-number'." in caplog.text
    assert (
        "JOB_COMPLETION_COUNT is not set. It should be set to the number of "
        "validation files in the directory (4)." in caplog.text
    )
    assert mock_run.call_args.args[0].config_dir is None
    assert os.path.basename(mock_run.call_args.args[0].config_file) == "0002.yaml"


@pytest.mark.parametrize("invalid_count", ["0", "-1"])
@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "data_validation.cli_tools.list_validations",
    return_value=["0000.yaml", "0001.yaml", "0002.yaml", "0003.yaml"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    side_effect=lambda *a, **kw: argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_non_positive_count(
    mock_args, mock_list, mock_build, mock_run, caplog, monkeypatch, invalid_count
):
    """Test that a zero or negative task count logs warnings and still runs the indexed file."""
    caplog.set_level(logging.WARNING)
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "2")
    monkeypatch.setenv("JOB_COMPLETION_COUNT", invalid_count)
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)

    assert f"Ignoring invalid task count {invalid_count}." in caplog.text
    assert (
        "JOB_COMPLETION_COUNT is not set. It should be set to the number of "
        "validation files in the directory (4)." in caplog.text
    )
    assert mock_run.call_args.args[0].config_dir is None
    assert os.path.basename(mock_run.call_args.args[0].config_file) == "0002.yaml"

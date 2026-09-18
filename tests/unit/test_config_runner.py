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
    passed to it. Build Config Managers reads the yaml files and builds the validation configs,
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
    1. No warnings
    2. run validation called as though config file is provided (config_dir is None)
    3. run validation config file name corresponds to value of JOB_COMPLETION_INDEX
    4. One config manager created for validation
    """
    caplog.set_level(logging.WARNING)
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "2")
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
def test_config_runner_dynamic_chunking(
    mock_args, mock_list, mock_build, mock_run, caplog, monkeypatch
):
    """Test dynamic round-robin chunking in Kubernetes Completion Environment.
    With job_count=3 and job_index=1, and 5 files:
    Sorted files: a.yaml, b.yaml, c.yaml, d.yaml, e.yaml
    Task 1 should get: b.yaml (idx 1), e.yaml (idx 4)
    """
    caplog.set_level(logging.INFO)
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "1")
    monkeypatch.setenv("JOB_COMPLETION_COUNT", "3")
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)

    # Assert no warnings, check info logs
    assert "Running in parallel completions mode with dynamic chunking." in caplog.text
    assert "Task 1 of 3. Assigned 2 of 5 files." in caplog.text

    # Assert validations called twice for the correct files in round-robin order
    assert mock_run.call_count == 2
    # Call 1: b.yaml
    assert mock_build.call_args_list[0][0][1] == "b.yaml"
    # Call 2: e.yaml
    assert mock_build.call_args_list[1][0][1] == "e.yaml"
    # Unlike the legacy 1-to-1 path, config_dir remains set because the file
    # names are relative to it.
    assert mock_run.call_args.args[0].config_dir == CONFIG_RUNNER_ARGS_3["config_dir"]


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
def test_config_runner_dynamic_chunking_failures(
    mock_args, mock_list, mock_build, mock_run, caplog, monkeypatch
):
    """Test dynamic round-robin chunking with failures in one of the validations."""
    mock_run.side_effect = [ValueError("Boom!"), 10]
    caplog.set_level(logging.ERROR)
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "1")
    monkeypatch.setenv("JOB_COMPLETION_COUNT", "3")
    args = cli_tools.get_parsed_args()
    caplog.clear()
    with pytest.raises(exceptions.ValidationException) as e_info:
        config_runner.config_runner(args)

    # Assert error is logged for b.yaml
    assert "Error 'Boom!' occurred while running config file b.yaml." in caplog.text
    # The exception is trapped, so the log entry has to carry the stack trace,
    # otherwise it is lost for good and cannot be diagnosed from container logs.
    assert caplog.records[0].exc_info is not None
    assert "Traceback (most recent call last)" in caplog.text
    # But both b.yaml and e.yaml should still be processed
    assert mock_run.call_count == 2
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
def test_config_runner_dynamic_chunking_cloud_run(
    mock_args, mock_list, mock_build, mock_run, caplog, monkeypatch
):
    """Test dynamic round-robin chunking using the Cloud Run environment variables.
    With task_count=2 and task_index=0, and 5 files:
    Sorted files: a.yaml, b.yaml, c.yaml, d.yaml, e.yaml
    Task 0 should get: a.yaml (idx 0), c.yaml (idx 2), e.yaml (idx 4)
    """
    caplog.set_level(logging.INFO)
    monkeypatch.setenv("CLOUD_RUN_TASK_INDEX", "0")
    monkeypatch.setenv("CLOUD_RUN_TASK_COUNT", "2")
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)

    assert "Task 0 of 2. Assigned 3 of 5 files." in caplog.text
    assert mock_run.call_count == 3
    assert [_[0][1] for _ in mock_build.call_args_list] == [
        "a.yaml",
        "c.yaml",
        "e.yaml",
    ]


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
def test_config_runner_dynamic_chunking_no_files(
    mock_args, mock_list, mock_build, mock_run, caplog, monkeypatch
):
    """Test that a task with more tasks than config files runs nothing but says so.
    With job_count=5 and job_index=4, and only 3 files, this task has no work to do.
    """
    caplog.set_level(logging.WARNING)
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "4")
    monkeypatch.setenv("JOB_COMPLETION_COUNT", "5")
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)

    assert "Task 4 has no config files to run" in caplog.text
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
def test_config_runner_dynamic_chunking_empty_config_dir(
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
def test_config_runner_dynamic_chunking_negative_index(
    mock_args, mock_list, mock_build, mock_run, monkeypatch
):
    """Test that a negative task index is rejected rather than treated as a slice offset.

    all_files[-1::3] would otherwise return the last file, re-running work already
    assigned to another task.
    """
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "-1")
    monkeypatch.setenv("JOB_COMPLETION_COUNT", "3")
    args = cli_tools.get_parsed_args()
    with pytest.raises(ValueError) as e_info:
        config_runner.config_runner(args)

    assert "Task index -1 cannot be negative" in str(e_info.value)
    assert mock_run.call_count == 0


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_legacy_negative_index(
    mock_args, mock_build, mock_run, monkeypatch
):
    """Test that a negative task index is also rejected on the legacy 1-to-1 path.

    Without the check this builds a nonsensical '-001.yaml' config file name.
    """
    monkeypatch.setenv("CLOUD_RUN_TASK_INDEX", "-2")
    args = cli_tools.get_parsed_args()
    with pytest.raises(ValueError) as e_info:
        config_runner.config_runner(args)

    assert "Task index -2 cannot be negative" in str(e_info.value)
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
def test_config_runner_dynamic_chunking_invalid_count(
    mock_args, mock_list, mock_build, mock_run, caplog, monkeypatch
):
    """Test that an invalid task count is ignored in favour of the legacy 1-to-1 mapping."""
    caplog.set_level(logging.WARNING)
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "2")
    monkeypatch.setenv("JOB_COMPLETION_COUNT", "not-a-number")
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)

    assert "Ignoring invalid task count 'not-a-number'" in caplog.text
    # Legacy behaviour, one file matching the index, config_dir folded into the path.
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
def test_config_runner_dynamic_chunking_non_positive_count(
    mock_args, mock_list, mock_build, mock_run, caplog, monkeypatch, invalid_count
):
    """Test that a zero or negative task count logs a warning and falls back to legacy mode."""
    caplog.set_level(logging.WARNING)
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "2")
    monkeypatch.setenv("JOB_COMPLETION_COUNT", invalid_count)
    args = cli_tools.get_parsed_args()
    caplog.clear()
    config_runner.config_runner(args)

    assert (
        f"Ignoring invalid task count {invalid_count}, falling back to one config file per task."
        in caplog.text
    )
    assert mock_run.call_args.args[0].config_dir is None
    assert os.path.basename(mock_run.call_args.args[0].config_file) == "0002.yaml"


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "data_validation.cli_tools.list_validations",
    return_value=["table_a.yaml", "table_b.yaml"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_legacy_missing_job_completion_count(
    mock_args, mock_list, mock_build, mock_run, monkeypatch
):
    """Test that non-sequentially numbered YAML files without JOB_COMPLETION_COUNT raise ValueError."""
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "0")
    args = cli_tools.get_parsed_args()
    with pytest.raises(ValueError) as e_info:
        config_runner.config_runner(args)

    assert (
        "Please set JOB_COMPLETION_COUNT in the Job manifest to run validations in parallel"
        in str(e_info.value)
    )
    assert mock_run.call_count == 0


@mock.patch("data_validation.config_runner.run_validations")
@mock.patch(
    "data_validation.config_runner.build_config_managers_from_yaml",
    return_value=["config dict from one file"],
)
@mock.patch(
    "argparse.ArgumentParser.parse_args",
    return_value=argparse.Namespace(**CONFIG_RUNNER_ARGS_3),
)
def test_config_runner_dynamic_chunking_index_out_of_bounds(
    mock_args, mock_build, mock_run, monkeypatch
):
    """Test that job_index >= job_count raises ValueError."""
    monkeypatch.setenv("JOB_COMPLETION_INDEX", "3")
    monkeypatch.setenv("JOB_COMPLETION_COUNT", "3")
    args = cli_tools.get_parsed_args()
    with pytest.raises(ValueError) as e_info:
        config_runner.config_runner(args)

    assert "Task index 3 is not valid for a job of 3 tasks." in str(e_info.value)
    assert mock_run.call_count == 0

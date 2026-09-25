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

import logging
import os

from data_validation import (
    cli_tools,
    clients,
    consts,
    exceptions,
    state_manager,
)
from data_validation.config_manager import ConfigManager
from data_validation.validation_runner import run_validations


def _get_kube_completions_task_count():
    """Return the total number of parallel tasks in this job, or None if unknown.

    Kubernetes injects JOB_COMPLETION_INDEX into indexed Jobs but does not inject a
    matching count, therefore JOB_COMPLETION_COUNT has to be set by the user in the
    Job manifest. Cloud Run sets both CLOUD_RUN_TASK_INDEX and CLOUD_RUN_TASK_COUNT
    automatically.
    """
    job_count_str = os.environ.get("JOB_COMPLETION_COUNT") or os.environ.get(
        "CLOUD_RUN_TASK_COUNT"
    )
    if not job_count_str:
        return None

    try:
        job_count = int(job_count_str)
    except ValueError:
        logging.warning(
            "Ignoring invalid task count '%s'.",
            job_count_str,
        )
        return None

    if job_count <= 0:
        logging.warning(
            "Ignoring invalid task count %d.",
            job_count,
        )
        return None

    return job_count


def _list_validation_files(config_dir: str) -> list:
    """Return the sorted validation YAML file names found in config_dir.

    An empty directory is treated as an error rather than as zero work to do. This
    matters most for Cloud Storage, where a misspelt prefix such as
    gs://my-bucket/typo-dir/ is not an error in itself, it simply matches no objects.
    Without this check DVT would report a successful run having validated nothing.
    """
    config_file_names = sorted(cli_tools.list_validations(config_dir=config_dir))
    if not config_file_names:
        raise ValueError(
            f"No validation YAML files found in config directory: {config_dir}"
        )
    return config_file_names


def _run_config_files(args, config_file_names: list):
    """Run the validations held in each of config_file_names, in sequence.

    A failure in one file does not prevent the remaining files from running, instead all
    failures are logged and a single exception is raised once they have all been attempted.
    """
    errors = False
    for file in config_file_names:
        try:
            logging.info(
                "Currently running the validation for YAML file: %s",
                file,
            )
            config_managers = build_config_managers_from_yaml(args, file)
            run_validations(args, config_managers)
        except Exception as e:
            errors = True
            logging.error(
                "Error '%s' occurred while running config file %s. Skipping it for now.",
                str(e),
                file,
                exc_info=True,
            )
    if errors:
        raise exceptions.ValidationException(
            "Some of the validations raised an exception"
        )


def config_runner(args):
    """Config Runner is where the decision is made to run validations from one or more files.
    One file can produce multiple validations - for example when more than one set of tables are being
    validated between the source and target. If multiple files are to be run, it is possible to run
    them concurrently in a Kubernetes / Cloud Run environment.
    If the user wants that, they need to specify a -kc or --kube-completions which tells DVT that it
    is one of a number of parallel tasks, identified by the index number provided in the
    JOB_COMPLETION_INDEX (for Kubernetes) or CLOUD_RUN_TASK_INDEX (for Cloud Run) environment
    variable. This environment variable is set by the Kubernetes/Cloud Run container orchestrator.

    All validation YAML files in the config directory are sorted by name and this task runs the
    file corresponding to its index in the sorted list. The number of tasks/completions should
    match the number of validation YAML files in the directory.
    """
    if args.config_dir:
        if args.kube_completions and (
            ("JOB_COMPLETION_INDEX" in os.environ.keys())
            or ("CLOUD_RUN_TASK_INDEX" in os.environ.keys())
        ):
            # Running in Kubernetes / Cloud Run Job completions - only run the YAML file for this index
            job_index = (
                int(os.environ.get("JOB_COMPLETION_INDEX"))
                if "JOB_COMPLETION_INDEX" in os.environ.keys()
                else int(os.environ.get("CLOUD_RUN_TASK_INDEX"))
            )
            # A negative index should never happen but we defend against it just in case.
            if job_index < 0:
                raise ValueError(f"Task index {job_index} cannot be negative.")

            all_files = _list_validation_files(args.config_dir)
            num_files = len(all_files)

            job_count = _get_kube_completions_task_count()
            if job_count is None:
                logging.warning(
                    "JOB_COMPLETION_COUNT is not set. It should be set to the number "
                    "of validation files in the directory (%d).",
                    num_files,
                )
            elif job_count < num_files:
                logging.warning(
                    "Task count (%d) is less than the number of validation files in "
                    "the directory (%d). The validation is likely to be partial as "
                    "not all the validations in the directory will be executed.",
                    job_count,
                    num_files,
                )

            if job_index < num_files:
                config_file_name = all_files[job_index]
                config_file_path = (
                    f"{args.config_dir}{config_file_name}"
                    if args.config_dir.endswith("/")
                    else f"{args.config_dir}/{config_file_name}"
                )
                logging.info(
                    "Running validation for index %d: YAML file %s",
                    job_index,
                    config_file_name,
                )
                setattr(args, "config_dir", None)
                setattr(args, "config_file", config_file_path)
                config_managers = build_config_managers_from_yaml(
                    args, config_file_path
                )
                run_validations(args, config_managers)
            else:
                logging.error(
                    "No validation file found for index %d (directory contains %d "
                    "validation files). Too many jobs/tasks have been instantiated.",
                    job_index,
                    num_files,
                )
        else:
            if args.kube_completions:
                logging.warning(
                    "--kube-completions or -kc specified, however not running in Kubernetes Job completion, check your command line."
                )
            config_file_names = _list_validation_files(args.config_dir)
            _run_config_files(args, config_file_names)
    else:
        if args.kube_completions:
            logging.warning(
                "--kube-completions or -kc specified, which requires a config directory, however a specific config file is provided."
            )
        config_file_path = cli_tools.get_arg_config_file(args)
        config_managers = build_config_managers_from_yaml(args, config_file_path)
        run_validations(args, config_managers)


def build_config_managers_from_yaml(args, config_file_path):
    """Returns List[ConfigManager] instances ready to be executed."""
    if args.config_dir:
        yaml_configs = cli_tools.get_validation(config_file_path, args.config_dir)
    else:
        yaml_configs = cli_tools.get_validation(config_file_path)

    mgr = state_manager.StateManager()
    source_conn = mgr.get_connection_config(yaml_configs[consts.YAML_SOURCE])
    target_conn = mgr.get_connection_config(yaml_configs[consts.YAML_TARGET])

    source_client = clients.get_data_client(source_conn)
    target_client = clients.get_data_client(target_conn)

    config_managers = []
    for config in yaml_configs[consts.YAML_VALIDATIONS]:
        config[consts.CONFIG_SOURCE_CONN] = source_conn
        config[consts.CONFIG_TARGET_CONN] = target_conn
        config[consts.CONFIG_RESULT_HANDLER] = yaml_configs[consts.YAML_RESULT_HANDLER]
        config_manager = ConfigManager(
            config, source_client, target_client, verbose=args.verbose
        )
        config_manager.config[consts.CONFIG_FILE] = config_file_path
        config_managers.append(config_manager)

    return config_managers

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

import json
import logging

from data_validation import clients, exceptions, util
from data_validation.config_manager import ConfigManager
from data_validation.data_validation import DataValidation


def run_validation(config_manager: ConfigManager, dry_run=False, verbose=False):
    """Run a single validation.

    Args:
        config_manager (ConfigManager): Validation config manager instance.
        dry_run (bool): Print source and target SQL to stdout in lieu of validation.
        verbose (bool): Validation setting to log queries run.
    """
    # Only use cached connection for SQLAlchemy backends that manage reconnects for us.
    source_client = (
        config_manager.source_client
        if clients.is_sqlalchemy_backend(config_manager.source_client)
        else None
    )
    target_client = (
        config_manager.target_client
        if clients.is_sqlalchemy_backend(config_manager.target_client)
        else None
    )
    # Though trims string based primary key value has been deprecated, some YAML files may still have that property set.
    if config_manager.trim_string_pks():
        logging.warning(
            "Trim String Primary Keys has been deprecated, validation results may vary"
        )
    try:
        with DataValidation(
            config_manager.config,
            validation_builder=None,
            result_handler=None,
            verbose=verbose,
            cached_source_client=source_client,
            cached_target_client=target_client,
        ) as validator:

            if dry_run:
                print(
                    json.dumps(
                        {
                            "source_query": util.ibis_table_to_sql(
                                validator.validation_builder.get_source_query(),
                                source_client,
                            ),
                            "target_query": util.ibis_table_to_sql(
                                validator.validation_builder.get_target_query(),
                                target_client,
                            ),
                        },
                        indent=4,
                    )
                )
            else:
                validator.execute()
    except Exception as e:
        if config_manager.full_source_table:
            raise exceptions.ValidationException(
                f"Validation failed for table '{config_manager.full_source_table}': {e}"
            ) from e
        raise


def run_validations(args, config_managers):
    """Run and manage a series of validations.

    Args:
        config_managers (list[ConfigManager]): List of config manager instances.
    """
    for config_manager in config_managers:
        run_validation(config_manager, dry_run=args.dry_run, verbose=args.verbose)

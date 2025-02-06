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
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor
import ibis.backends.pandas
import pandas
import uuid

from data_validation import combiner, consts, metadata, util
from data_validation.config_manager import ConfigManager
from data_validation.query_builder.random_row_builder import RandomRowBuilder
from data_validation.schema_validation import SchemaValidation
from data_validation.validation_builder import ValidationBuilder

""" The DataValidation class is where the code becomes source/target aware

    The class builds specific source and target clients and is likely where someone would go to
    customize their validation process.

    data_validator = DataValidation(builder, source_config, target_config, result_handler=None, verbose=False)
"""


class DataValidation(object):
    def __init__(
        self,
        config,
        validation_builder=None,
        schema_validator=None,
        result_handler=None,
        verbose=False,
        source_client: ibis.backends.base.BaseBackend = None,
        target_client: ibis.backends.base.BaseBackend = None,
    ):
        """Initialize a DataValidation client

        Args:
            config (dict): The validation config used for the comparison.
            validation_builder (ValidationBuilder): Optional instance of a ValidationBuilder.
            schema_validator (SchemaValidation): Optional instance of a SchemaValidation.
            result_handler (ResultHandler): Optional instance of as ResultHandler client.
            verbose (bool): If verbose, the Data Validation client will print the queries run.
            source_client: Optional client to avoid unnecessary connections,
            target_client: Optional client to avoid unnecessary connections,
        """
        self.verbose = verbose
        self._fresh_connections = not bool(source_client and target_client)

        # Data Client Management
        self.config = config

        self.config_manager = ConfigManager(
            config,
            source_client=source_client,
            target_client=target_client,
            verbose=self.verbose,
        )

        self.run_metadata = metadata.RunMetadata()
        self.run_metadata.labels = self.config_manager.labels

        # Use a generated uuid for the run_id if None was supplied via config
        self.run_metadata.run_id = self.config_manager.run_id or str(uuid.uuid4())

        # Initialize Validation Builder if None was supplied
        self.validation_builder = validation_builder or ValidationBuilder(
            self.config_manager
        )

        self.schema_validator = schema_validator or SchemaValidation(
            self.config_manager, run_metadata=self.run_metadata, verbose=self.verbose
        )

        # Initialize the default Result Handler if None was supplied
        self.result_handler = result_handler or self.config_manager.get_result_handler()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self._fresh_connections and hasattr(self, "config_manager"):
            self.config_manager.close_client_connections()

    # TODO(dhercher) we planned on shifting this to use an Execution Handler.
    # Leaving to to swast on the design of how this should look.
    def execute(self):
        """Execute Queries and Store Results"""
        # Apply random row filter before validations run
        if self.config_manager.use_random_rows():
            util.timed_call("Random row filter", self._add_random_row_filter)

        # Run correct execution for the given validation type
        if self.config_manager.validation_type == consts.ROW_VALIDATION:
            grouped_fields = self.validation_builder.pop_grouped_fields()
            result_df = self.execute_recursive_validation(
                self.validation_builder, grouped_fields
            )
        elif self.config_manager.validation_type == consts.SCHEMA_VALIDATION:
            """Perform only schema validation"""
            result_df = util.timed_call(
                "Schema validation", self.schema_validator.execute
            )
        else:
            result_df = self._execute_validation(
                self.validation_builder, process_in_memory=True
            )

        # Call Result Handler to Manage Results
        return self.result_handler.execute(result_df)

    def _add_random_row_filter(self):
        """Add random row filters to the validation builder."""
        if not self.config_manager.primary_keys:
            raise ValueError("Primary Keys are required for Random Row Filters")

        # Filter for only first primary key (multi-pk filter not supported)
        source_pk_column = self.config_manager.primary_keys[0][
            consts.CONFIG_SOURCE_COLUMN
        ]
        target_pk_column = self.config_manager.primary_keys[0][
            consts.CONFIG_TARGET_COLUMN
        ]

        randomRowBuilder = RandomRowBuilder(
            [source_pk_column],
            self.config_manager.random_row_batch_size(),
        )

        if (self.config_manager.validation_type == consts.CUSTOM_QUERY) and (
            self.config_manager.custom_query_type == consts.ROW_VALIDATION.lower()
        ):
            query = randomRowBuilder.compile_custom_query(
                self.config_manager.source_client,
                self.config_manager.source_query,
            )
        else:
            query = randomRowBuilder.compile(
                self.config_manager.source_client,
                self.config_manager.source_schema,
                self.config_manager.source_table,
                self.validation_builder.source_builder,
            )

        # Check if source table's primary key is BINARY, if so then
        # force cast the id columns to STRING (HEX).
        binary_conversion_required = False
        if query[source_pk_column].type().is_binary():
            binary_conversion_required = True
            query = query.mutate(
                **{source_pk_column: query[source_pk_column].cast("string")}
            )

        if self.config_manager.trim_string_pks():
            query = query.mutate(**{source_pk_column: query[source_pk_column].rstrip()})

        random_rows = self.config_manager.source_client.execute(query)
        if len(random_rows) == 0:
            return

        random_values = list(random_rows[source_pk_column])
        if binary_conversion_required:
            # For binary ids we have a list of hex strings for our IN list.
            # Each of these needs to be cast back to binary.
            random_values = [ibis.literal(_).cast("binary") for _ in random_values]

        filter_field = {
            consts.CONFIG_TYPE: consts.FILTER_TYPE_ISIN,
            consts.CONFIG_FILTER_SOURCE_COLUMN: source_pk_column,
            consts.CONFIG_FILTER_SOURCE_VALUE: random_values,
            consts.CONFIG_FILTER_TARGET_COLUMN: target_pk_column,
            consts.CONFIG_FILTER_TARGET_VALUE: random_values,
        }

        self.validation_builder.add_filter(filter_field)

    def query_too_large(self, rows_df, grouped_fields):
        """Return bool to dictate if another level of recursion
        would create a too large result set.

        Rules to define too large are:
            - If any grouped fields remain, return False.
                (assumes user added logical sized groups)
            - Else, if next group size is larger
                than the limit, return True.
            - Finally return False if no covered case occured.
        """
        if len(grouped_fields) > 1:
            return False

        try:
            count_df = rows_df[
                rows_df[consts.AGGREGATION_TYPE] == consts.CONFIG_TYPE_COUNT
            ]
            for row in count_df.to_dict(orient="row"):
                recursive_query_size = max(
                    float(row[consts.SOURCE_AGG_VALUE]),
                    float(row[consts.TARGET_AGG_VALUE]),
                )
                if recursive_query_size > self.config_manager.max_recursive_query_size:
                    logging.warning("Query result is too large for recursion: %s", row)
                    return True
        except Exception:
            logging.warning("Recursive values could not be cast to float.")
            return False

        return False

    def execute_recursive_validation(self, validation_builder, grouped_fields):
        """Recursive execution for Row validations.

        This method executes aggregate queries, such as sum-of-hashes, on the
        source and target tables. Where they differ, add to the GROUP BY
        clause recursively until the individual row differences can be
        identified.
        """
        process_in_memory = self.config_manager.process_in_memory()
        past_results = []
        if len(grouped_fields) > 0:
            validation_builder.add_query_group(grouped_fields[0])
            result_df = self._execute_validation(
                validation_builder, process_in_memory=process_in_memory
            )

            for grouped_key in result_df[consts.GROUP_BY_COLUMNS].unique():
                # Validations are viewed separtely, but queried together.
                # We must treat them as a single item which failed or succeeded.
                group_suceeded = True
                grouped_key_df = result_df[
                    result_df[consts.GROUP_BY_COLUMNS] == grouped_key
                ]

                if self.query_too_large(grouped_key_df, grouped_fields):
                    past_results.append(grouped_key_df)
                    continue

                for row in grouped_key_df.to_dict(orient="row"):
                    if row[consts.SOURCE_AGG_VALUE] == row[consts.TARGET_AGG_VALUE]:
                        continue
                    else:
                        group_suceeded = False
                        break

                if group_suceeded:
                    past_results.append(grouped_key_df)
                else:
                    recursive_validation_builder = validation_builder.clone()
                    self._add_recursive_validation_filter(
                        recursive_validation_builder, row
                    )
                    past_results.append(
                        self.execute_recursive_validation(
                            recursive_validation_builder, grouped_fields[1:]
                        )
                    )
        elif self.config_manager.primary_keys and len(grouped_fields) == 0:
            past_results.append(
                self._execute_validation(
                    validation_builder, process_in_memory=process_in_memory
                )
            )

        # elif self.config_manager.primary_keys:
        #     validation_builder.add_config_query_groups(self.config_manager.primary_keys)
        #     validation_builder.add_config_query_groups(grouped_fields)

        else:
            warnings.warn(
                "WARNING: No Primary Keys Suppplied in Row Validation", UserWarning
            )
            return None

        return pandas.concat(past_results)

    def _add_recursive_validation_filter(self, validation_builder, row):
        """Return ValidationBuilder Configured for Next Recursive Search"""
        group_by_columns = json.loads(row[consts.GROUP_BY_COLUMNS])
        for alias, value in group_by_columns.items():
            filter_field = {
                consts.CONFIG_TYPE: consts.FILTER_TYPE_EQUALS,
                consts.CONFIG_FILTER_SOURCE_COLUMN: validation_builder.get_grouped_alias_source_column(
                    alias
                ),
                consts.CONFIG_FILTER_SOURCE_VALUE: value,
                consts.CONFIG_FILTER_TARGET_COLUMN: validation_builder.get_grouped_alias_target_column(
                    alias
                ),
                consts.CONFIG_FILTER_TARGET_VALUE: value,
            }
            validation_builder.add_filter(filter_field)

    def _execute_validation(self, validation_builder, process_in_memory=True):
        """Execute Against a Supplied Validation Builder"""

        self.run_metadata.validations = validation_builder.get_metadata()

        source_query = validation_builder.get_source_query()
        target_query = validation_builder.get_target_query()

        join_on_fields = (
            set(validation_builder.get_primary_keys())
            if (self.config_manager.validation_type == consts.ROW_VALIDATION)
            or (
                self.config_manager.validation_type == consts.CUSTOM_QUERY
                and self.config_manager.custom_query_type == "row"
            )
            else set(validation_builder.get_group_aliases())
        )

        # If row validation from YAML, compare source and target agg values
        is_value_comparison = (
            self.config_manager.validation_type == consts.ROW_VALIDATION
            or (
                self.config_manager.validation_type == consts.CUSTOM_QUERY
                and self.config_manager.custom_query_type == "row"
            )
        )

        if process_in_memory:
            futures = []
            with ThreadPoolExecutor() as executor:
                # Submit the two query network calls concurrently
                futures.append(
                    executor.submit(
                        util.timed_call,
                        "Source query",
                        self.config_manager.source_client.execute,
                        source_query,
                    )
                )
                futures.append(
                    executor.submit(
                        util.timed_call,
                        "Target query",
                        self.config_manager.target_client.execute,
                        target_query,
                    )
                )
                source_df = futures[0].result()
                target_df = futures[1].result()

            pandas_client = ibis.pandas.connect(
                {combiner.DEFAULT_SOURCE: source_df, combiner.DEFAULT_TARGET: target_df}
            )

            try:
                result_df = util.timed_call(
                    "Generate report",
                    combiner.generate_report,
                    pandas_client,
                    self.run_metadata,
                    pandas_client.table(combiner.DEFAULT_SOURCE),
                    pandas_client.table(combiner.DEFAULT_TARGET),
                    join_on_fields=join_on_fields,
                    is_value_comparison=is_value_comparison,
                    verbose=self.verbose,
                )
            except Exception as e:
                if self.verbose:
                    logging.error("-- ** Logging Source DF ** --")
                    logging.error(source_df.dtypes)
                    logging.error(source_df)
                    logging.error("-- ** Logging Target DF ** --")
                    logging.error(target_df.dtypes)
                    logging.error(target_df)
                raise e
        else:
            result_df = combiner.generate_report(
                self.config_manager.source_client,
                self.run_metadata,
                source_query,
                target_query,
                join_on_fields=join_on_fields,
                is_value_comparison=is_value_comparison,
                verbose=self.verbose,
            )

        return result_df

    def combine_data(self, source_df, target_df, join_on_fields):
        """TODO: Return List of Dictionaries"""
        # Clean Data to Standardize
        if join_on_fields:
            df = source_df.merge(
                target_df,
                how="outer",
                on=join_on_fields,
                suffixes=(consts.INPUT_SUFFIX, consts.OUTPUT_SUFFIX),
            )
        else:
            df = source_df.join(
                target_df,
                how="outer",
                lsuffix=consts.INPUT_SUFFIX,
                rsuffix=consts.OUTPUT_SUFFIX,
            )
        return df

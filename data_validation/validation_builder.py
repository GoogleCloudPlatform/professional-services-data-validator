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

from copy import deepcopy

from data_validation import consts, metadata
from data_validation.query_builder.custom_query_builder import CustomQueryBuilder
from data_validation.query_builder.query_builder import (
    AggregateField,
    CalculatedField,
    FilterField,
    GroupedField,
    QueryBuilder,
)


class ValidationBuilder(object):
    def __init__(self, config_manager):
        """Initialize a ValidationBuilder client which supplies the
            source and target queries to run.

        Args:
            config_manager (ConfigManager): A validation ConfigManager instance
            source_client (IbisClient): The Ibis client for the source DB
            target_client (IbisClient): The Ibis client for the target DB
            verbose (Bool): If verbose, the Data Validation client will print queries run
        """
        self._metadata = {}
        self.config_manager = config_manager
        self.verbose = self.config_manager.verbose
        self.validation_type = self.config_manager.validation_type

        self.source_client = self.config_manager.source_client
        self.target_client = self.config_manager.target_client

        self.source_builder = self.get_query_builder(self.validation_type)
        self.target_builder = self.get_query_builder(self.validation_type)

        self.primary_keys = {}
        self.group_aliases = {}
        self.calculated_aliases = {}
        self.comparison_fields = {}

        self.add_config_aggregates()
        self.add_config_query_groups()
        self.add_config_calculated_fields()
        self.add_comparison_fields()
        self.add_config_filters()
        self.add_primary_keys()
        self.add_query_limit()

    def clone(self):
        cloned_builder = ValidationBuilder(self.config_manager)

        cloned_builder.source_builder = deepcopy(self.source_builder)
        cloned_builder.target_builder = deepcopy(self.target_builder)
        cloned_builder.group_aliases = deepcopy(self.group_aliases)
        cloned_builder.calculated_aliases = deepcopy(self.calculated_aliases)
        cloned_builder.comparison_fields = deepcopy(self.comparison_fields)
        cloned_builder._metadata = deepcopy(self._metadata)

        return cloned_builder

    @staticmethod
    def get_query_builder(validation_type):
        """Return Query Builder object given validation type"""
        if validation_type in [
            "Column",
            "GroupedColumn",
            "Row",
            "Schema",
            "Custom-query",
        ]:
            builder = QueryBuilder.build_count_validator()
        else:
            msg = "Validation Builder supplied unknown type: %s" % validation_type
            raise ValueError(msg)

        return builder

    def get_metadata(self):
        """Metadata about the run and any validations it contains.

        The validation metadata is populated as validations are added to this
        builder.

        Returns:
            Dict[str, ValidationMetadata]:
                A dictionary of validation name to ValidationMetadata.
        """
        return self._metadata

    def get_group_aliases(self):
        """Return List of String Aliases"""
        return list(self.group_aliases.keys())

    def get_primary_keys(self):
        """Return List of String Aliases"""
        # do we need this?
        return list(self.primary_keys.keys())

    def get_calculated_aliases(self):
        """Return List of String Aliases"""
        return self.calculated_aliases.keys()

    def get_comparison_fields(self):
        """Return List of String Aliases"""
        return self.comparison_fields.keys()

    def get_grouped_alias_source_column(self, alias):
        return self.group_aliases[alias][consts.CONFIG_SOURCE_COLUMN]

    def get_grouped_alias_target_column(self, alias):
        return self.group_aliases[alias][consts.CONFIG_TARGET_COLUMN]

    def add_config_aggregates(self):
        """Add Aggregations to Query"""
        aggregate_fields = self.config_manager.aggregates
        for aggregate_field in aggregate_fields:
            self.add_aggregate(aggregate_field)

    def add_config_calculated_fields(self):
        """Add calculated fields to Query"""
        calc_fields = self.config_manager.calculated_fields
        if calc_fields is not None:
            for calc_field in calc_fields:
                self.add_calc(calc_field)

    def add_primary_keys(self, primary_keys=None):
        primary_keys = primary_keys or self.config_manager.primary_keys
        for field in primary_keys:
            self.add_primary_key(field)

    def add_comparison_fields(self, comparison_fields=None):
        comparison_fields = comparison_fields or self.config_manager.comparison_fields
        for field in comparison_fields:
            self.add_comparison_field(field)

    def add_config_query_groups(self, query_groups=None):
        """Add Grouped Columns to Query"""
        grouped_fields = query_groups or self.config_manager.query_groups
        for grouped_field in grouped_fields:
            self.add_query_group(grouped_field)

    def add_config_filters(self):
        """Add Filters to Query"""
        filter_fields = self.config_manager.filters
        for filter_field in filter_fields:
            self.add_filter(filter_field)

    def add_aggregate(self, aggregate_field):
        """Add Aggregate Field to Queries

        Args:
            aggregate_field (dict): A dictionary with source, target, and aggregation info.
        """
        alias = aggregate_field[consts.CONFIG_FIELD_ALIAS]
        source_field_name = aggregate_field[consts.CONFIG_SOURCE_COLUMN]
        target_field_name = aggregate_field[consts.CONFIG_TARGET_COLUMN]
        aggregate_type = aggregate_field.get(consts.CONFIG_TYPE)
        cast = aggregate_field.get(consts.CONFIG_CAST)
        if not hasattr(AggregateField, aggregate_type):
            raise Exception("Unknown Aggregation Type: {}".format(aggregate_type))

        source_agg = getattr(AggregateField, aggregate_type)(
            field_name=source_field_name, alias=alias, cast=cast
        )
        target_agg = getattr(AggregateField, aggregate_type)(
            field_name=target_field_name, alias=alias, cast=cast
        )

        self.source_builder.add_aggregate_field(source_agg)
        self.target_builder.add_aggregate_field(target_agg)
        self._metadata[alias] = metadata.ValidationMetadata(
            validation_type=self.validation_type,
            aggregation_type=aggregate_type,
            source_table_schema=self.config_manager.source_schema,
            source_table_name=self.config_manager.source_table,
            target_table_schema=self.config_manager.target_schema,
            target_table_name=self.config_manager.target_table,
            source_column_name=source_field_name,
            target_column_name=target_field_name,
            primary_keys=self.config_manager.get_primary_keys_list(),
            num_random_rows=self.config_manager.get_random_row_batch_size(),
            threshold=self.config_manager.threshold,
        )

    def pop_grouped_fields(self):
        """Return grouped fields and reset configs."""
        self.source_builder.grouped_fields = []
        self.target_builder.grouped_fields = []
        self.group_aliases = {}

        return self.config_manager.query_groups

    def add_query_group(self, grouped_field):
        """Add Grouped Field to Query

        Args:
            grouped_field (Dict): An object with source, target, and cast info
        """
        alias = grouped_field[consts.CONFIG_FIELD_ALIAS]
        source_field_name = grouped_field[consts.CONFIG_SOURCE_COLUMN]
        target_field_name = grouped_field[consts.CONFIG_TARGET_COLUMN]
        cast = grouped_field.get(consts.CONFIG_CAST)

        source_field = GroupedField(
            field_name=source_field_name, alias=alias, cast=cast
        )
        target_field = GroupedField(
            field_name=target_field_name, alias=alias, cast=cast
        )

        self.source_builder.add_grouped_field(source_field)
        self.target_builder.add_grouped_field(target_field)
        self.group_aliases[alias] = grouped_field

    def add_primary_key(self, primary_key):
        """Add ComparionField to Queries

        Args:
            primary_key (Dict): An object with source, target, and cast info
        """
        source_field_name = primary_key[consts.CONFIG_SOURCE_COLUMN]
        target_field_name = primary_key[consts.CONFIG_TARGET_COLUMN]
        # grab calc field metadata
        alias = primary_key[consts.CONFIG_FIELD_ALIAS]
        # check if valid calc field and return correct object
        self.source_builder.add_comparison_field(source_field_name)
        self.target_builder.add_comparison_field(target_field_name)
        self.primary_keys[alias] = primary_key

    def add_filter(self, filter_field):
        """Add FilterField to Queries

        Args:
            filter_field (Dict): An object with source and target filter details
        """
        if filter_field[consts.CONFIG_TYPE] == consts.FILTER_TYPE_CUSTOM:
            source_filter = FilterField.custom(
                filter_field[consts.CONFIG_FILTER_SOURCE]
            )
            target_filter = FilterField.custom(
                filter_field[consts.CONFIG_FILTER_TARGET]
            )
        elif filter_field[consts.CONFIG_TYPE] == consts.FILTER_TYPE_EQUALS:
            source_filter = FilterField.equal_to(
                filter_field[consts.CONFIG_FILTER_SOURCE_COLUMN],
                filter_field[consts.CONFIG_FILTER_SOURCE_VALUE],
            )
            target_filter = FilterField.equal_to(
                filter_field[consts.CONFIG_FILTER_TARGET_COLUMN],
                filter_field[consts.CONFIG_FILTER_TARGET_VALUE],
            )
        elif filter_field[consts.CONFIG_TYPE] == consts.FILTER_TYPE_ISIN:
            source_filter = FilterField.isin(
                filter_field[consts.CONFIG_FILTER_SOURCE_COLUMN],
                filter_field[consts.CONFIG_FILTER_SOURCE_VALUE],
            )
            target_filter = FilterField.isin(
                filter_field[consts.CONFIG_FILTER_TARGET_COLUMN],
                filter_field[consts.CONFIG_FILTER_TARGET_VALUE],
            )

        # TODO(issues/40): Add metadata around filters
        self.source_builder.add_filter_field(source_filter)
        self.target_builder.add_filter_field(target_filter)

    def add_comparison_field(self, comparison_field):
        """Add ComparionField to Queries

        Args:
            comparison_field (Dict): An object with source, target, and cast info
        """
        source_field_name = comparison_field[consts.CONFIG_SOURCE_COLUMN]
        target_field_name = comparison_field[consts.CONFIG_TARGET_COLUMN]
        # grab calc field metadata
        alias = comparison_field[consts.CONFIG_FIELD_ALIAS]
        # check if valid calc field and return correct object
        self.source_builder.add_comparison_field(source_field_name)
        self.target_builder.add_comparison_field(target_field_name)
        self._metadata[alias] = metadata.ValidationMetadata(
            aggregation_type=None,
            validation_type=self.validation_type,
            source_table_schema=self.config_manager.source_schema,
            source_table_name=self.config_manager.source_table,
            target_table_schema=self.config_manager.target_schema,
            target_table_name=self.config_manager.target_table,
            source_column_name=source_field_name,
            target_column_name=target_field_name,
            primary_keys=self.config_manager.get_primary_keys_list(),
            num_random_rows=self.config_manager.get_random_row_batch_size(),
            threshold=self.config_manager.threshold,
        )

    def add_calc(self, calc_field):
        """Add CalculatedField to Queries

        Args:
            calc_field (Dict): An object with source, target, and cast info
        """
        # prepare source and target payloads
        source_config = deepcopy(calc_field)
        source_fields = calc_field[consts.CONFIG_CALCULATED_SOURCE_COLUMNS]
        target_config = deepcopy(calc_field)
        target_fields = calc_field[consts.CONFIG_CALCULATED_TARGET_COLUMNS]
        # grab calc field metadata
        alias = calc_field[consts.CONFIG_FIELD_ALIAS]
        calc_type = calc_field[consts.CONFIG_TYPE]
        # check if valid calc field and return correct object
        if not hasattr(CalculatedField, calc_type):
            raise Exception("Unknown Calculation Type: {}".format(calc_type))
        source_field = getattr(CalculatedField, calc_type)(
            config=source_config, fields=source_fields
        )
        target_field = getattr(CalculatedField, calc_type)(
            config=target_config, fields=target_fields
        )
        self.source_builder.add_calculated_field(source_field)
        self.target_builder.add_calculated_field(target_field)
        # register calc field under alias
        self.calculated_aliases[alias] = calc_field

    def get_source_query(self):
        """Return query for source validation"""
        source_config = {
            "data_client": self.source_client,
            "schema_name": self.config_manager.source_schema,
            "table_name": self.config_manager.source_table,
        }
        if self.validation_type == consts.CUSTOM_QUERY:
            source_input_query = self.get_query_from_file(
                self.config_manager.source_query_file[0]
            )
            if self.config_manager.custom_query_type == "row":
                calculated_query = CustomQueryBuilder().compile_custom_query(
                    source_input_query, source_config
                )
                query = self.source_client.sql(calculated_query)
            elif self.config_manager.custom_query_type == "column":
                source_aggregate_query = "SELECT "
                for aggregate in self.config_manager.aggregates:
                    source_aggregate_query += (
                        CustomQueryBuilder().get_aggregation_query(
                            aggregate.get("type"), aggregate.get("target_column")
                        )
                    )
                source_aggregate_query = (
                    CustomQueryBuilder().get_wrapper_aggregation_query(
                        source_aggregate_query, source_input_query
                    )
                )
                query = self.source_client.sql(source_aggregate_query)
            else:
                raise ValueError(
                    "Expected custom query type to be column or row, got an unacceptable value. "
                    f"Input custom query type: {self.config_manager.custom_query_type}"
                )
        else:
            query = self.source_builder.compile(**source_config)
        if self.verbose:
            print(source_config)
            print("-- ** Source Query ** --")
            print(query.compile())

        return query

    def get_target_query(self):
        """Return query for source validation"""
        target_config = {
            "data_client": self.target_client,
            "schema_name": self.config_manager.target_schema,
            "table_name": self.config_manager.target_table,
        }
        if self.validation_type == consts.CUSTOM_QUERY:
            target_input_query = self.get_query_from_file(
                self.config_manager.target_query_file[0]
            )
            if self.config_manager.custom_query_type == "row":
                calculated_query = CustomQueryBuilder().compile_custom_query(
                    target_input_query, target_config
                )
                query = self.target_client.sql(calculated_query)
            elif self.config_manager.custom_query_type == "column":
                target_aggregate_query = "SELECT "
                for aggregate in self.config_manager.aggregates:
                    target_aggregate_query += (
                        CustomQueryBuilder().get_aggregation_query(
                            aggregate.get("type"), aggregate.get("target_column")
                        )
                    )
                target_aggregate_query = (
                    CustomQueryBuilder().get_wrapper_aggregation_query(
                        target_aggregate_query, target_input_query
                    )
                )
                query = self.target_client.sql(target_aggregate_query)
            else:
                raise ValueError(
                    "Expected custom query type to be column or row, got an unacceptable value. "
                    f"Input custom query type: {self.config_manager.custom_query_type}"
                )
        else:
            query = self.target_builder.compile(**target_config)
        if self.verbose:
            print(target_config)
            print("-- ** Target Query ** --")
            print(query.compile())

        return query

    def add_query_limit(self):
        """Add a limit to the query results

        **WARNING** this can skew results and should be used carefully
        """
        limit = self.config_manager.query_limit
        self.source_builder.limit = limit
        self.target_builder.limit = limit

    def get_query_from_file(self, filename):
        """Return query from input file"""
        query = ""
        try:
            file = open(filename, "r")
            query = file.read()
        except IOError:
            print("Cannot read query file: ", filename)

        if not query or query.isspace():
            raise ValueError(
                "Expected file with sql query, got empty file or file with white spaces. "
                f"input file: {filename}"
            )
        file.close()
        return query

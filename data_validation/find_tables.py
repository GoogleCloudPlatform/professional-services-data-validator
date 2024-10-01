# Copyright 2024 Google LLC
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
from typing import TYPE_CHECKING

from data_validation import (
    cli_tools,
    clients,
    consts,
    jellyfish_distance,
    state_manager,
)

if TYPE_CHECKING:
    import ibis


def _compare_match_tables(source_table_map, target_table_map, score_cutoff=0.8) -> list:
    """Return dict config object from matching tables."""
    # TODO(dhercher): evaluate if improved comparison and score cutoffs should be used.
    table_configs = []

    target_keys = target_table_map.keys()
    for source_key in source_table_map:
        target_key = jellyfish_distance.extract_closest_match(
            source_key, target_keys, score_cutoff=score_cutoff
        )
        if target_key is None:
            continue

        table_config = {
            consts.CONFIG_SCHEMA_NAME: source_table_map[source_key][
                consts.CONFIG_SCHEMA_NAME
            ],
            consts.CONFIG_TABLE_NAME: source_table_map[source_key][
                consts.CONFIG_TABLE_NAME
            ],
            consts.CONFIG_TARGET_SCHEMA_NAME: target_table_map[target_key][
                consts.CONFIG_SCHEMA_NAME
            ],
            consts.CONFIG_TARGET_TABLE_NAME: target_table_map[target_key][
                consts.CONFIG_TABLE_NAME
            ],
        }
        table_configs.append(table_config)

    return table_configs


def _get_table_map(client: "ibis.backends.base.BaseBackend", allowed_schemas=None):
    """Return dict with searchable keys for table matching."""
    table_map = {}
    table_objs = clients.get_all_tables(client, allowed_schemas=allowed_schemas)

    for table_obj in table_objs:
        table_key = ".".join([t for t in table_obj if t])
        table_map[table_key] = {
            consts.CONFIG_SCHEMA_NAME: table_obj[0],
            consts.CONFIG_TABLE_NAME: table_obj[1],
        }

    return table_map


def get_mapped_table_configs(
    source_client: "ibis.backends.base.BaseBackend",
    target_client: "ibis.backends.base.BaseBackend",
    allowed_schemas: list = None,
    score_cutoff: int = 1,
) -> list:
    """Get table list from each client and match them together into a single list of dicts."""
    source_table_map = _get_table_map(source_client, allowed_schemas=allowed_schemas)
    target_table_map = _get_table_map(target_client)
    return _compare_match_tables(
        source_table_map, target_table_map, score_cutoff=score_cutoff
    )


def find_tables_using_string_matching(args) -> str:
    """Return JSON String with matched tables for use in validations."""
    score_cutoff = args.score_cutoff or 1

    mgr = state_manager.StateManager()
    source_client = clients.get_data_client(mgr.get_connection_config(args.source_conn))
    target_client = clients.get_data_client(mgr.get_connection_config(args.target_conn))

    allowed_schemas = cli_tools.get_arg_list(args.allowed_schemas)
    table_configs = get_mapped_table_configs(
        source_client,
        target_client,
        allowed_schemas=allowed_schemas,
        score_cutoff=score_cutoff,
    )
    return json.dumps(table_configs)


def expand_tables_of_asterisk(
    tables_list: list,
    source_client: "ibis.backends.base.BaseBackend",
    target_client: "ibis.backends.base.BaseBackend",
) -> list:
    """Pre-processes tables_mapping expanding any entries that are "schema.*". A shorthand for "find-tables" command.

    We can be very specific in this function, we only expand arguments that are:
      {"schema_name": (str), "table_name": "*"}.
    No partial wildcards or args that include target_schema/table_name are expanded.

    Args:
        tables_list (list[dict]): List of schema/table name dicts.
        source_client: Ibis client we can use to get a table list.
        target_client: Ibis client we can use to get a table list.

    Returns:
        list: New version of tables_list with expanded "table_name": "*" entries.
    """
    new_list = []
    for mapping in tables_list:
        if (
            mapping
            and mapping[consts.CONFIG_SCHEMA_NAME]
            and mapping[consts.CONFIG_TABLE_NAME] == "*"
            # Looking for schema.* without a target side qualifier.
            and not mapping.get(consts.CONFIG_TARGET_SCHEMA_NAME, None)
            and not mapping.get(consts.CONFIG_TARGET_TABLE_NAME, None)
        ):
            # Expand the "*" to all tables in the schema.
            expanded_tables = get_mapped_table_configs(
                source_client,
                target_client,
                allowed_schemas=[mapping[consts.CONFIG_SCHEMA_NAME]],
            )
            new_list.extend(expanded_tables)
        else:
            new_list.append(mapping)
    return new_list

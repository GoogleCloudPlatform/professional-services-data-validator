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
    util,
)

if TYPE_CHECKING:
    import ibis


def _casefold_keys(d: dict) -> dict:
    """Return a copy of d with keys casefolded, where this does not cause a collision.

    If a casefolded key already exists (e.g. both "HR" and "hr" are present) the
    original key is retained so both entries remain distinct.
    """
    folded = dict(d)
    for key in [_ for _ in folded if _ != _.casefold()]:
        if key.casefold() not in folded:
            # Only lower case the key if there isn't one already.
            folded[key.casefold()] = folded.pop(key)
    return folded


def _get_casefolded(folded: dict, key: str, default=None):
    """Lookup key in a dict produced by _casefold_keys.

    An exact-case match is preferred, otherwise the casefolded key is used.
    """
    if not key:
        return default
    if key in folded:
        return folded[key]
    return folded.get(key.casefold(), default)


def _resolve_schema_map(schema_map: dict, source_schemas: set) -> dict:
    """Re-key a user supplied schema_map using the real source schema names.

    The keys of schema_map come from the user and may differ in case from the
    real schema names, e.g. "PROD_HR" vs "prod_hr". Keys are resolved using the
    same rule as _filter_schemas: an exact-case match is preferred, otherwise the
    casefolded name is used. Keys that do not resolve are kept as-is.
    """
    if not schema_map:
        return {}
    folded_sources = _casefold_keys({s: s for s in source_schemas if s})
    resolved = {}
    # Exact-case keys first so they take priority over casefolded matches.
    for key in sorted(schema_map, key=lambda k: k not in source_schemas):
        real_schema = _get_casefolded(folded_sources, key, default=key)
        resolved.setdefault(real_schema, schema_map[key])
    return resolved


def _compare_match_tables(
    source_table_map: dict,
    target_table_map: dict,
    score_cutoff: float = 0.8,
    schema_map: dict = None,
) -> list:
    """Return dict config object from matching tables."""
    # TODO(dhercher): evaluate if improved comparison and score cutoffs should be used.
    table_configs = []
    schema_map = _resolve_schema_map(
        schema_map,
        {_[consts.CONFIG_SCHEMA_NAME] for _ in source_table_map.values()},
    )

    target_keys = target_table_map.keys()
    for source_key in source_table_map:
        source_schema = source_table_map[source_key][consts.CONFIG_SCHEMA_NAME]
        source_table = source_table_map[source_key][consts.CONFIG_TABLE_NAME]

        # Apply schema mapping if it exists, when there is a mapping
        # lookup_key is {mapped_schema}.{source_name} which should then
        # find a match in target_keys.
        lookup_schema = schema_map.get(source_schema, source_schema)
        lookup_key = f"{lookup_schema}.{source_table}".casefold()

        target_key = jellyfish_distance.extract_closest_match(
            lookup_key, target_keys, score_cutoff=score_cutoff
        )
        if target_key is None:
            continue

        table_config = {
            consts.CONFIG_SCHEMA_NAME: source_schema,
            consts.CONFIG_TABLE_NAME: source_table,
            consts.CONFIG_TARGET_SCHEMA_NAME: target_table_map[target_key][
                consts.CONFIG_SCHEMA_NAME
            ],
            consts.CONFIG_TARGET_TABLE_NAME: target_table_map[target_key][
                consts.CONFIG_TABLE_NAME
            ],
        }
        table_configs.append(table_config)

    return table_configs


def _get_table_map_from_obj_list(table_objs: list) -> dict:
    """Convert list of schema, table tuples into dict with searchable keys for table matching."""
    table_map = {}
    for table_obj in table_objs:
        table_key = ".".join([t for t in table_obj if t])
        table_map[table_key] = {
            consts.CONFIG_SCHEMA_NAME: table_obj[0],
            consts.CONFIG_TABLE_NAME: table_obj[1],
        }

    # Post process table_map and lower case table_keys, if possible.
    return _casefold_keys(table_map)


def _filter_schemas(schemas: list, allowed_schemas: list = None) -> list:
    """Filter database schemas using exact, case-insensitive matching.

    An exact-case match is preferred, otherwise the casefolded name is used.
    This mirrors the casefolding applied to table keys in _get_table_map_from_obj_list.
    """
    if not allowed_schemas:
        return None

    schema_map = _casefold_keys({s: s for s in schemas if s})

    matched_schemas = []
    for allowed_schema in allowed_schemas:
        matched_schema = _get_casefolded(schema_map, allowed_schema)
        if matched_schema is not None and matched_schema not in matched_schemas:
            matched_schemas.append(matched_schema)

    return matched_schemas


def _get_table_map(
    client: "ibis.backends.base.BaseBackend",
    allowed_schemas=None,
    include_views=False,
) -> dict:
    """Return dict with searchable keys for table matching."""
    if allowed_schemas:
        allowed_schemas = _filter_schemas(
            clients.list_databases(client),
            allowed_schemas,
        )
    else:
        allowed_schemas = None
    table_objs = clients.get_all_tables(
        client,
        allowed_schemas=allowed_schemas,
        tables_only=(not include_views),
    )
    return _get_table_map_from_obj_list(table_objs)


def get_mapped_table_configs(
    source_client: "ibis.backends.base.BaseBackend",
    target_client: "ibis.backends.base.BaseBackend",
    allowed_schemas: list = None,
    include_views: bool = False,
    score_cutoff: int = 1,
    schema_map: dict = None,
) -> list:
    """Get table list from each client and match them together into a single list of dicts."""

    def _local_get_mapped_table_configs():
        source_table_map = _get_table_map(
            source_client, allowed_schemas=allowed_schemas, include_views=include_views
        )
        target_allowed_schemas = None
        if allowed_schemas and score_cutoff >= 1:
            # Pruning target schemas is only lossless for exact (case-insensitive)
            # matching. With fuzzy matching a schema.table key can score above
            # score_cutoff even when the schema names alone do not, so all target
            # schemas must be considered.
            target_allowed_schemas = [
                (schema_map or {}).get(s, s) for s in allowed_schemas
            ]
        target_table_map = _get_table_map(
            target_client,
            allowed_schemas=target_allowed_schemas,
            include_views=include_views,
        )
        return _compare_match_tables(
            source_table_map,
            target_table_map,
            score_cutoff=score_cutoff,
            schema_map=schema_map,
        )

    return util.timed_call("Find tables", _local_get_mapped_table_configs)


def find_tables_using_string_matching(args) -> str:
    """Return JSON String with matched tables for use in validations."""
    score_cutoff = args.score_cutoff or 1

    mgr = state_manager.StateManager()
    source_client = clients.get_data_client(mgr.get_connection_config(args.source_conn))
    target_client = clients.get_data_client(mgr.get_connection_config(args.target_conn))

    allowed_schemas_raw = cli_tools.get_arg_list(args.allowed_schemas) or []
    allowed_schemas = []
    schema_map = {}
    for schema in allowed_schemas_raw:
        if "=" in schema:
            schema, target_schema = schema.split("=", 1)
            schema_map[schema] = target_schema
        allowed_schemas.append(schema)

    table_configs = get_mapped_table_configs(
        source_client,
        target_client,
        allowed_schemas=allowed_schemas,
        include_views=args.include_views,
        score_cutoff=score_cutoff,
        schema_map=schema_map,
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
                include_views=False,
            )
            new_list.extend(expanded_tables)
        else:
            new_list.append(mapping)
    return new_list

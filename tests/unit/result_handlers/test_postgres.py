import threading
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import sqlalchemy

from data_validation.result_handlers import postgres


def test_concurrent_missing_results_table_is_created_once():
    state_lock = threading.Lock()
    first_lookup_barrier = threading.Barrier(2)
    table_created = False
    lookup_count_by_client = {}
    created_tables = []

    def get_ibis_table(client, schema_name, table_name):
        with state_lock:
            lookup_number = lookup_count_by_client.get(id(client), 0)
            lookup_count_by_client[id(client)] = lookup_number + 1
            exists = table_created

        if lookup_number == 0:
            first_lookup_barrier.wait(timeout=5)
            raise sqlalchemy.exc.NoSuchTableError(table_name)
        if not exists:
            raise sqlalchemy.exc.NoSuchTableError(table_name)
        return mock.sentinel.table

    def create_table(table_name, schema):
        nonlocal table_created
        with state_lock:
            if table_created:
                raise AssertionError("Results table was created more than once")
            table_created = True
            created_tables.append((table_name, schema))

    clients = [mock.Mock(), mock.Mock()]
    for client in clients:
        client.create_table.side_effect = create_table

    handlers = [
        postgres.PostgresResultHandler(client, table_id="results") for client in clients
    ]
    result_df = mock.Mock(empty=True)

    with mock.patch.object(
        postgres.clients, "get_ibis_table", side_effect=get_ibis_table
    ):
        with ThreadPoolExecutor(max_workers=2) as executor:
            list(
                executor.map(
                    lambda handler: handler._insert_postgres(result_df), handlers
                )
            )

    assert len(created_tables) == 1
    assert created_tables[0] == ("results", postgres.RESULTS_TABLE_SCHEMA)
    assert all(lookup_count_by_client[id(client)] == 2 for client in clients)

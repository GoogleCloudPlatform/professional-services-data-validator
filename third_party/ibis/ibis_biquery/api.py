from ibis.backends.bigquery import Backend as BigQueryBackend


def _list_primary_key_columns(self, database: str, table: str) -> list:
    """Return a list of primary key column names."""
    # TODO: Related to issue-1253, it's not clear if this is possible, we should revisit if it becomes a requirement.
    return None


def _dvt_list_tables(self, like=None, database=None):
    return self.list_tables(like=like, database=database)


BigQueryBackend.list_primary_key_columns = _list_primary_key_columns
BigQueryBackend.dvt_list_tables = _dvt_list_tables

"""Tests for ClickHouse ibis backend patches.

Following DVT's testing patterns:
- Test patch registration (verify monkey-patch applied)
- Test handler function behavior directly with minimal mocks
- Focus on testing OUR code, not ibis framework
"""

from unittest import mock

import pytest


def get_module_under_test():
    """Attempt to import the ClickHouse ibis module."""
    try:
        from third_party.ibis import ibis_clickhouse
    except ModuleNotFoundError:
        # We don't necessarily have the ClickHouse driver installed.
        # Tests will be skipped when the driver is missing.
        ibis_clickhouse = None

    return ibis_clickhouse


@pytest.fixture
def module_under_test():
    """Fixture providing the ClickHouse ibis module."""
    return get_module_under_test()


@pytest.mark.skipif(not get_module_under_test(), reason="No ClickHouse driver")
def test_import(module_under_test):
    """Test that the ClickHouse ibis module can be imported."""
    assert module_under_test is not None


@pytest.mark.skipif(not get_module_under_test(), reason="No ClickHouse driver")
def test_array_join_patch_registered(module_under_test):
    """Test that the ARRAY JOIN patch is registered in translate_rel."""
    import ibis.expr.operations as ops
    from ibis.backends.clickhouse.compiler import relations

    # Verify that ops.SQLQueryResult has a custom handler registered
    assert ops.SQLQueryResult in relations.translate_rel.registry

    # Get the registered function
    handler = relations.translate_rel.registry[ops.SQLQueryResult]

    # Verify it's our patched version
    assert handler.__name__ == "_query_clickhouse_patched"


@pytest.mark.skipif(not get_module_under_test(), reason="No ClickHouse driver")
def test_clickhouse_specific_keywords_defined(module_under_test):
    """Test that ClickHouse-specific keywords are defined."""
    assert hasattr(module_under_test, "CLICKHOUSE_SPECIFIC_KEYWORDS")
    keywords = module_under_test.CLICKHOUSE_SPECIFIC_KEYWORDS

    # Verify expected keywords are present
    assert "ARRAY JOIN" in keywords
    assert "FINAL" in keywords
    assert "GLOBAL JOIN" in keywords


@pytest.mark.skipif(not get_module_under_test(), reason="No ClickHouse driver")
def test_array_join_uses_command_wrapper(module_under_test):
    """Test that queries with ARRAY JOIN use Command wrapper (bypass parsing)."""
    from sqlglot import exp

    # Get the handler function directly
    handler = module_under_test._query_clickhouse_patched

    # Create minimal mock with just the query attribute
    mock_op = mock.Mock()
    mock_op.query = """
        SELECT col1, item
        FROM table1
        ARRAY JOIN array_col AS item
        WHERE col1 > 10
    """

    # Call handler directly
    aliases = {mock_op: "_test_alias"}
    result = handler(mock_op, aliases=aliases)

    # Verify it returns a Subquery
    assert isinstance(result, exp.Subquery)

    # Verify it uses Command (bypass parsing for ARRAY JOIN)
    assert isinstance(result.this, exp.Command)

    # Light verification: alias is applied
    result_sql = result.sql(dialect="clickhouse")
    assert "_test_alias" in result_sql


@pytest.mark.skipif(not get_module_under_test(), reason="No ClickHouse driver")
def test_standard_sql_uses_parsed_select(module_under_test):
    """Test that standard SQL without ClickHouse syntax uses normal parsing."""
    from sqlglot import exp

    # Get the handler function directly
    handler = module_under_test._query_clickhouse_patched

    # Create mock with standard SQL (no ClickHouse-specific syntax)
    mock_op = mock.Mock()
    mock_op.query = """
        SELECT col1, col2
        FROM table1
        WHERE col1 > 10
    """

    # Call handler directly
    aliases = {mock_op: "_standard_alias"}
    result = handler(mock_op, aliases=aliases)

    # Verify it returns a Subquery
    assert isinstance(result, exp.Subquery)

    # Verify it uses parsed Select (not Command)
    assert isinstance(result.this, exp.Select)


@pytest.mark.skipif(not get_module_under_test(), reason="No ClickHouse driver")
def test_final_modifier_uses_command_wrapper(module_under_test):
    """Test that queries with FINAL modifier use Command wrapper."""
    from sqlglot import exp

    handler = module_under_test._query_clickhouse_patched

    mock_op = mock.Mock()
    mock_op.query = """
        SELECT col1, col2
        FROM table1 FINAL
        WHERE col1 > 10
    """

    aliases = {mock_op: "_final_alias"}
    result = handler(mock_op, aliases=aliases)

    # Verify Command is used (bypass parser for FINAL)
    assert isinstance(result, exp.Subquery)
    assert isinstance(result.this, exp.Command)


@pytest.mark.skipif(not get_module_under_test(), reason="No ClickHouse driver")
def test_global_join_uses_command_wrapper(module_under_test):
    """Test that queries with GLOBAL JOIN use Command wrapper."""
    from sqlglot import exp

    handler = module_under_test._query_clickhouse_patched

    mock_op = mock.Mock()
    mock_op.query = """
        SELECT t1.col1, t2.col2
        FROM table1 t1
        GLOBAL INNER JOIN table2 t2 ON t1.id = t2.id
    """

    aliases = {mock_op: "_global_alias"}
    result = handler(mock_op, aliases=aliases)

    # Verify Command is used (bypass parser for GLOBAL JOIN)
    assert isinstance(result, exp.Subquery)
    assert isinstance(result.this, exp.Command)


@pytest.mark.skipif(not get_module_under_test(), reason="No ClickHouse driver")
def test_case_insensitive_keyword_detection(module_under_test):
    """Test that keyword detection is case-insensitive."""
    from sqlglot import exp

    handler = module_under_test._query_clickhouse_patched

    # Test with lowercase "array join"
    mock_op = mock.Mock()
    mock_op.query = "SELECT * FROM table array join array_col AS item"

    result = handler(mock_op, aliases={})

    # Should still detect and use Command
    assert isinstance(result.this, exp.Command)


@pytest.mark.skipif(not get_module_under_test(), reason="No ClickHouse driver")
def test_complex_query_with_cte_and_array_join(module_under_test):
    """Test complex query with CTE and ARRAY JOIN."""
    from sqlglot import exp

    handler = module_under_test._query_clickhouse_patched

    mock_op = mock.Mock()
    mock_op.query = """
        WITH raw_data AS (
            SELECT project_id, labels, report
            FROM source_table
        )
        SELECT
            project_id,
            report_value.cost AS cost
        FROM raw_data AS T
        ARRAY JOIN T.report AS report_value
        WHERE T.project_id = 'test-project'
    """

    aliases = {mock_op: "_complex_alias"}
    result = handler(mock_op, aliases=aliases)

    # Verify successful translation with Command wrapper
    assert isinstance(result, exp.Subquery)
    assert isinstance(result.this, exp.Command)

"""
Unit tests for packages/clickhouse_loader.py
Tests: verify_clickhouse_data, transform_data_python
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

# ---------------------------------------------------------------------------
# Stub heavy dependencies before importing the module under test
# ---------------------------------------------------------------------------
airflow_stub = MagicMock()
airflow_stub.exceptions.AirflowException = Exception
sys.modules.setdefault('airflow', airflow_stub)
sys.modules.setdefault('airflow.exceptions', airflow_stub.exceptions)

clickhouse_connect_stub = MagicMock()
sys.modules['clickhouse_connect'] = clickhouse_connect_stub

# Pre-configure the module-level client with a MagicMock so the import
# of clickhouse_loader does not attempt a real TCP connection.
_mock_client = MagicMock()
clickhouse_connect_stub.get_client.return_value = _mock_client

import packages.clickhouse_loader as loader  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reset_client():
    """Return a fresh MagicMock and wire it into the module via the factory."""
    mock = MagicMock()
    loader.get_clickhouse_client = lambda: mock
    return mock


def _make_query_result(rows):
    result = MagicMock()
    result.result_rows = rows
    return result


# ---------------------------------------------------------------------------
# verify_clickhouse_data
# ---------------------------------------------------------------------------

class TestVerifyClickhouseData(unittest.TestCase):

    def test_returns_metadata_when_table_exists_with_data(self):
        client = _reset_client()

        # SHOW TABLES → contains 'raw_sales'
        client.query.side_effect = [
            _make_query_result([('raw_sales',), ('other_table',)]),   # SHOW TABLES
            _make_query_result([('order_id', 'String'), ('price', 'Float64')]),  # DESCRIBE
            _make_query_result([(500,)]),   # SELECT count()
        ]

        result = loader.verify_clickhouse_data()

        self.assertIn('count', result)
        self.assertEqual(result['count'], 500)
        self.assertIn('columns', result)
        self.assertIn('order_id', result['columns'])

    def test_raises_when_raw_sales_empty(self):
        client = _reset_client()

        client.query.side_effect = [
            _make_query_result([('raw_sales',)]),   # SHOW TABLES
            _make_query_result([('order_id', 'String')]),  # DESCRIBE
            _make_query_result([(0,)]),   # SELECT count() → 0 rows
        ]

        with self.assertRaises(Exception) as ctx:
            loader.verify_clickhouse_data()
        self.assertIn('No data found', str(ctx.exception))

    def test_raises_when_exception_from_clickhouse(self):
        client = _reset_client()
        client.query.side_effect = RuntimeError('connection refused')

        with self.assertRaises(Exception) as ctx:
            loader.verify_clickhouse_data()
        self.assertIn('Failed to verify', str(ctx.exception))


# ---------------------------------------------------------------------------
# transform_data_python
# ---------------------------------------------------------------------------

class TestTransformDataPython(unittest.TestCase):

    def _setup_client_for_success(self):
        """Simulate a successful run: command() succeeds, query() returns counts."""
        client = _reset_client()
        client.command.return_value = None

        count_result = _make_query_result([(100,)])
        client.query.return_value = count_result
        return client

    def test_returns_true_on_success(self):
        self._setup_client_for_success()
        result = loader.transform_data_python()
        self.assertTrue(result)

    def test_creates_all_expected_tables(self):
        client = self._setup_client_for_success()
        loader.transform_data_python()

        all_commands = ' '.join(
            str(c) for c in client.command.call_args_list
        )
        for expected in (
            'stg_sales',
            'customer_metrics',
            'product_analysis',
            'daily_sales_summary',
            'monthly_sales_summary',
            'customer_product_affinity',
        ):
            self.assertIn(expected, all_commands, f"Expected '{expected}' in SQL commands")

    def test_drops_tables_before_creating(self):
        """Each analytics table should be dropped before creation."""
        client = self._setup_client_for_success()
        loader.transform_data_python()

        drop_calls = [
            str(c) for c in client.command.call_args_list
            if 'DROP TABLE IF EXISTS' in str(c)
        ]
        self.assertGreater(len(drop_calls), 0, "Expected DROP TABLE IF EXISTS calls")

    def test_raises_airflow_exception_on_command_failure(self):
        client = _reset_client()
        client.command.side_effect = RuntimeError('ClickHouse error')

        with self.assertRaises(Exception) as ctx:
            loader.transform_data_python()
        self.assertIn('Failed to transform data', str(ctx.exception))

    def test_verifies_result_counts_after_transform(self):
        """Should call SELECT count() on each transformed table."""
        client = self._setup_client_for_success()
        loader.transform_data_python()

        query_calls = [str(c) for c in client.query.call_args_list]
        full_queries = ' '.join(query_calls)

        for table in (
            'customer_metrics',
            'product_analysis',
            'daily_sales_summary',
            'monthly_sales_summary',
            'customer_product_affinity',
        ):
            self.assertIn(table, full_queries, f"Expected count query for '{table}'")


# ---------------------------------------------------------------------------
# run_dbt_on_clickhouse (smoke test — it just returns True)
# ---------------------------------------------------------------------------

class TestRunDbtOnClickhouse(unittest.TestCase):

    def test_returns_true(self):
        result = loader.run_dbt_on_clickhouse()
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()

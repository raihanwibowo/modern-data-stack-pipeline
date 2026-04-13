"""
Unit tests for packages/summary_report.py
Tests: get_clickhouse_client, generate_summary_report
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

# ---------------------------------------------------------------------------
# Stub dependencies before importing the module under test
# ---------------------------------------------------------------------------
airflow_stub = MagicMock()
airflow_stub.exceptions.AirflowException = Exception
sys.modules.setdefault('airflow', airflow_stub)
sys.modules.setdefault('airflow.exceptions', airflow_stub.exceptions)

clickhouse_connect_stub = MagicMock()
sys.modules['clickhouse_connect'] = clickhouse_connect_stub

import packages.summary_report as summary  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_query_result(rows):
    result = MagicMock()
    result.result_rows = rows
    return result


def _make_client(
    raw_count=1000,
    tables=('raw_sales', 'customer_metrics', 'product_analysis',
            'daily_sales_summary', 'monthly_sales_summary',
            'customer_product_affinity'),
    customer_metrics_count=50,
    product_count=20,
    daily_count=30,
    monthly_count=12,
    affinity_count=200,
    top_customer=None,
    top_product=None,
    tier_breakdown=None,
):
    """Build a fully-configured mock ClickHouse client."""
    client = MagicMock()

    def _query(sql):
        sql = sql.strip()
        if 'SHOW TABLES' in sql:
            return _make_query_result([(t,) for t in tables])
        if 'count()' in sql.lower() and 'raw_sales' in sql and 'customer' not in sql:
            return _make_query_result([(raw_count,)])
        if 'count()' in sql.lower() and 'customer_metrics' in sql:
            return _make_query_result([(customer_metrics_count,)])
        if 'count()' in sql.lower() and 'product_analysis' in sql and 'tier' not in sql:
            return _make_query_result([(product_count,)])
        if 'count()' in sql.lower() and 'daily_sales_summary' in sql:
            return _make_query_result([(daily_count,)])
        if 'count()' in sql.lower() and 'monthly_sales_summary' in sql:
            return _make_query_result([(monthly_count,)])
        if 'count()' in sql.lower() and 'customer_product_affinity' in sql:
            return _make_query_result([(affinity_count,)])
        if 'product_tier' in sql:
            tb = tier_breakdown or [('Gold', 5), ('Silver', 10), ('Bronze', 5)]
            return _make_query_result(tb)
        if 'top_customer' in sql or ('customer_id' in sql and 'total_revenue' in sql and 'LIMIT 1' in sql):
            tc = top_customer or [('CUST_001', 9999.99)]
            return _make_query_result(tc)
        if 'top_product' in sql or ('product_id' in sql and 'product_tier' in sql and 'LIMIT 1' in sql):
            tp = top_product or [('PROD_001', 'Gold', 5000.0)]
            return _make_query_result(tp)
        # Default empty
        return _make_query_result([])

    client.query.side_effect = _query
    return client


# ---------------------------------------------------------------------------
# get_clickhouse_client
# ---------------------------------------------------------------------------

class TestGetClickhouseClient(unittest.TestCase):

    def test_creates_client_with_config(self):
        """get_clickhouse_client() passes ClickHouseConfig fields to clickhouse_connect"""
        mock_client = MagicMock()
        clickhouse_connect_stub.get_client.return_value = mock_client

        env = {
            'CLICKHOUSE_HOST': 'my-host',
            'CLICKHOUSE_HTTP_PORT': '8124',
            'CLICKHOUSE_USER': 'my_user',
            'CLICKHOUSE_PASSWORD': 'my_pass',
            'CLICKHOUSE_DATABASE': 'my_db',
        }
        with patch.dict(os.environ, env):
            result = summary.get_clickhouse_client()

        clickhouse_connect_stub.get_client.assert_called_once_with(
            host='my-host',
            port=8124,
            username='my_user',
            password='my_pass',
            database='my_db',
        )
        self.assertEqual(result, mock_client)


# ---------------------------------------------------------------------------
# generate_summary_report
# ---------------------------------------------------------------------------

class TestGenerateSummaryReport(unittest.TestCase):

    def test_returns_true_on_success(self):
        client = _make_client()
        with patch('packages.summary_report.get_clickhouse_client', return_value=client):
            result = summary.generate_summary_report()
        self.assertTrue(result)

    def test_queries_raw_sales_count(self):
        client = _make_client()
        with patch('packages.summary_report.get_clickhouse_client', return_value=client):
            summary.generate_summary_report()

        calls_str = ' '.join(str(c) for c in client.query.call_args_list)
        self.assertIn('raw_sales', calls_str)

    def test_queries_customer_metrics_when_table_exists(self):
        client = _make_client(tables=['raw_sales', 'customer_metrics'])
        with patch('packages.summary_report.get_clickhouse_client', return_value=client):
            summary.generate_summary_report()

        calls_str = ' '.join(str(c) for c in client.query.call_args_list)
        self.assertIn('customer_metrics', calls_str)

    def test_skips_customer_metrics_when_table_absent(self):
        client = _make_client(tables=['raw_sales'])  # customer_metrics not present
        with patch('packages.summary_report.get_clickhouse_client', return_value=client):
            result = summary.generate_summary_report()

        calls_str = ' '.join(str(c) for c in client.query.call_args_list)
        self.assertNotIn('customer_metrics', calls_str.lower().replace('show tables', ''))
        self.assertTrue(result)

    def test_queries_product_analysis_when_table_exists(self):
        client = _make_client(tables=['raw_sales', 'product_analysis'])
        with patch('packages.summary_report.get_clickhouse_client', return_value=client):
            summary.generate_summary_report()

        calls_str = ' '.join(str(c) for c in client.query.call_args_list)
        self.assertIn('product_analysis', calls_str)

    def test_returns_true_even_when_exception_raised(self):
        """generate_summary_report should NOT propagate exceptions (graceful degradation)"""
        client = MagicMock()
        client.query.side_effect = RuntimeError('Connection lost')

        with patch('packages.summary_report.get_clickhouse_client', return_value=client):
            result = summary.generate_summary_report()

        self.assertTrue(result)

    def test_shows_table_count(self):
        client = _make_client(
            tables=['raw_sales', 'customer_metrics'],
            raw_count=999,
        )
        with patch('packages.summary_report.get_clickhouse_client', return_value=client), \
             patch('builtins.print') as mock_print:
            summary.generate_summary_report()

        printed = ' '.join(str(c) for c in mock_print.call_args_list)
        self.assertIn('999', printed)

    def test_shows_all_table_sections(self):
        all_tables = [
            'raw_sales', 'customer_metrics', 'product_analysis',
            'daily_sales_summary', 'monthly_sales_summary', 'customer_product_affinity',
        ]
        client = _make_client(tables=all_tables)

        with patch('packages.summary_report.get_clickhouse_client', return_value=client), \
             patch('builtins.print') as mock_print:
            summary.generate_summary_report()

        printed = ' '.join(str(c) for c in mock_print.call_args_list)
        for label in ('Customer metrics', 'Product analysis', 'Daily sales', 'Monthly sales'):
            self.assertIn(label, printed, f"Expected '{label}' in printed output")

    def test_accepts_airflow_context_kwargs(self):
        """generate_summary_report(**context) should not raise for extra kwargs"""
        client = _make_client()
        with patch('packages.summary_report.get_clickhouse_client', return_value=client):
            result = summary.generate_summary_report(task_instance=MagicMock(), ds='2026-04-13')
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()

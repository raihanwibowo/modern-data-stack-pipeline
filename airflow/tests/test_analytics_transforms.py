"""
Unit tests for packages/analytics_transforms.py
Tests:
  - python_customer_analytics
  - python_product_cohorts
  - data_quality_checks
  - generate_summary
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock, call
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

# ---------------------------------------------------------------------------
# Stub heavy Airflow / DB dependencies before importing the module
# ---------------------------------------------------------------------------
airflow_stub = MagicMock()
airflow_stub.exceptions.AirflowException = Exception
airflow_stub.providers = MagicMock()
airflow_stub.providers.postgres = MagicMock()
airflow_stub.providers.postgres.hooks = MagicMock()
airflow_stub.providers.postgres.hooks.postgres = MagicMock()

sys.modules.setdefault('airflow', airflow_stub)
sys.modules.setdefault('airflow.exceptions', airflow_stub.exceptions)
sys.modules.setdefault('airflow.providers', airflow_stub.providers)
sys.modules.setdefault('airflow.providers.postgres', airflow_stub.providers.postgres)
sys.modules.setdefault('airflow.providers.postgres.hooks', airflow_stub.providers.postgres.hooks)
sys.modules.setdefault('airflow.providers.postgres.hooks.postgres', airflow_stub.providers.postgres.hooks.postgres)

import packages.analytics_transforms as analytics  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _customer_metrics_df():
    return pd.DataFrame({
        'customer_id': ['C1', 'C2', 'C3', 'C4', 'C5', 'C6'],
        'total_orders': [10, 5, 8, 3, 7, 2],
        'total_revenue': [1000.0, 500.0, 800.0, 300.0, 700.0, 200.0],
        'avg_order_value': [100.0, 100.0, 100.0, 100.0, 100.0, 100.0],
        'first_order_date': [
            '2024-01-01', '2024-02-01', '2024-03-01',
            '2024-04-01', '2024-05-01', '2024-06-01',
        ],
        'last_order_date': [
            '2024-12-01', '2024-11-01', '2024-10-01',
            '2024-09-01', '2024-08-01', '2024-07-01',
        ],
        'unique_products': [5, 3, 4, 2, 3, 1],
        'total_items_purchased': [50, 25, 40, 15, 35, 10],
    })


def _stg_sales_df():
    rows = []
    for i in range(10):
        rows.append({
            'order_id': f'O{i}',
            'customer_id': f'C{i % 3}',
            'product_id': f'P{i % 5}',
            'order_date': f'2024-0{(i % 9) + 1}-01',
            'total_amount': float(100 + i * 10),
            'quantity': i + 1,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# python_customer_analytics
# ---------------------------------------------------------------------------

class TestPythonCustomerAnalytics(unittest.TestCase):

    def _run(self, df):
        mock_hook = MagicMock()
        mock_engine = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = mock_engine

        with patch('packages.analytics_transforms.PostgresHook', return_value=mock_hook), \
             patch('packages.analytics_transforms.pd.read_sql', return_value=df), \
             patch.object(df.__class__, 'to_sql', return_value=None):
            return analytics.python_customer_analytics()

    def test_returns_row_count(self):
        df = _customer_metrics_df()
        mock_hook = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = MagicMock()

        with patch('packages.analytics_transforms.PostgresHook', return_value=mock_hook), \
             patch('packages.analytics_transforms.pd.read_sql', return_value=df), \
             patch.object(pd.DataFrame, 'to_sql', return_value=None):
            result = analytics.python_customer_analytics()

        self.assertEqual(result, len(df))

    def test_computes_customer_lifetime_days(self):
        df = _customer_metrics_df()
        mock_hook = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = MagicMock()

        with patch('packages.analytics_transforms.PostgresHook', return_value=mock_hook), \
             patch('packages.analytics_transforms.pd.read_sql', return_value=df), \
             patch.object(pd.DataFrame, 'to_sql', return_value=None):
            analytics.python_customer_analytics()

        # customer_lifetime_days should have been added to the dataframe
        self.assertIn('customer_lifetime_days', df.columns)
        self.assertTrue((df['customer_lifetime_days'] >= 0).all())

    def test_computes_rfm_scores(self):
        df = _customer_metrics_df()
        mock_hook = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = MagicMock()

        with patch('packages.analytics_transforms.PostgresHook', return_value=mock_hook), \
             patch('packages.analytics_transforms.pd.read_sql', return_value=df), \
             patch.object(pd.DataFrame, 'to_sql', return_value=None):
            analytics.python_customer_analytics()

        for col in ('recency_score', 'frequency_score', 'monetary_score', 'rfm_score'):
            self.assertIn(col, df.columns, f"Missing column: {col}")

    def test_writes_to_customer_rfm_analysis_table(self):
        df = _customer_metrics_df()
        mock_hook = MagicMock()
        mock_engine = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = mock_engine
        captured_calls = []

        original_to_sql = pd.DataFrame.to_sql

        def spy_to_sql(self_df, name, *args, **kwargs):
            captured_calls.append(name)

        with patch('packages.analytics_transforms.PostgresHook', return_value=mock_hook), \
             patch('packages.analytics_transforms.pd.read_sql', return_value=df), \
             patch.object(pd.DataFrame, 'to_sql', spy_to_sql):
            analytics.python_customer_analytics()

        self.assertIn('customer_rfm_analysis', captured_calls)


# ---------------------------------------------------------------------------
# python_product_cohorts
# ---------------------------------------------------------------------------

class TestPythonProductCohorts(unittest.TestCase):

    def test_returns_row_count(self):
        df = _stg_sales_df()
        mock_hook = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = MagicMock()

        with patch('packages.analytics_transforms.PostgresHook', return_value=mock_hook), \
             patch('packages.analytics_transforms.pd.read_sql', return_value=df), \
             patch.object(pd.DataFrame, 'to_sql', return_value=None):
            result = analytics.python_product_cohorts()

        self.assertIsInstance(result, int)
        self.assertGreater(result, 0)

    def test_writes_to_product_cohorts_table(self):
        df = _stg_sales_df()
        mock_hook = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = MagicMock()
        captured_calls = []

        def spy_to_sql(self_df, name, *args, **kwargs):
            captured_calls.append(name)

        with patch('packages.analytics_transforms.PostgresHook', return_value=mock_hook), \
             patch('packages.analytics_transforms.pd.read_sql', return_value=df), \
             patch.object(pd.DataFrame, 'to_sql', spy_to_sql):
            analytics.python_product_cohorts()

        self.assertIn('product_cohorts', captured_calls)

    def test_cohort_columns(self):
        """Result dataframe should contain expected columns"""
        df = _stg_sales_df()
        mock_hook = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = MagicMock()
        written_df = {}

        def capture_to_sql(self_df, name, *args, **kwargs):
            written_df[name] = self_df.copy()

        with patch('packages.analytics_transforms.PostgresHook', return_value=mock_hook), \
             patch('packages.analytics_transforms.pd.read_sql', return_value=df), \
             patch.object(pd.DataFrame, 'to_sql', capture_to_sql):
            analytics.python_product_cohorts()

        cohort = written_df.get('product_cohorts')
        self.assertIsNotNone(cohort)
        for col in ('product_id', 'cohort_month', 'orders', 'revenue', 'units_sold'):
            self.assertIn(col, cohort.columns)


# ---------------------------------------------------------------------------
# data_quality_checks
# ---------------------------------------------------------------------------

class TestDataQualityChecks(unittest.TestCase):

    def _make_read_sql(self, counts: dict):
        """Return a read_sql side_effect that maps table queries to counts"""
        def _read_sql(query, engine):
            for table, count in counts.items():
                if table in query:
                    return pd.DataFrame({'count': [count]})
            return pd.DataFrame({'count': [1]})
        return _read_sql

    def test_returns_dict_of_counts_on_success(self):
        counts = {
            'customer_segments': 10,
            'product_analysis': 5,
            'customer_rfm_analysis': 8,
            'product_cohorts': 4,
        }
        mock_hook = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = MagicMock()
        mock_ti = MagicMock()
        context = {'task_instance': mock_ti}

        with patch('packages.analytics_transforms.PostgresHook', return_value=mock_hook), \
             patch('packages.analytics_transforms.pd.read_sql', side_effect=self._make_read_sql(counts)):
            result = analytics.data_quality_checks(**context)

        self.assertIsInstance(result, dict)
        for table in counts:
            self.assertIn(table, result)

    def test_raises_when_table_empty(self):
        counts = {
            'customer_segments': 0,  # empty → should raise
            'product_analysis': 5,
            'customer_rfm_analysis': 8,
            'product_cohorts': 4,
        }
        mock_hook = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = MagicMock()

        with patch('packages.analytics_transforms.PostgresHook', return_value=mock_hook), \
             patch('packages.analytics_transforms.pd.read_sql', side_effect=self._make_read_sql(counts)):
            with self.assertRaises(ValueError) as ctx:
                analytics.data_quality_checks(task_instance=MagicMock())
        self.assertIn('customer_segments', str(ctx.exception))

    def test_pushes_results_to_xcom(self):
        counts = {
            'customer_segments': 3,
            'product_analysis': 3,
            'customer_rfm_analysis': 3,
            'product_cohorts': 3,
        }
        mock_hook = MagicMock()
        mock_hook.get_sqlalchemy_engine.return_value = MagicMock()
        mock_ti = MagicMock()

        with patch('packages.analytics_transforms.PostgresHook', return_value=mock_hook), \
             patch('packages.analytics_transforms.pd.read_sql', side_effect=self._make_read_sql(counts)):
            analytics.data_quality_checks(task_instance=mock_ti)

        mock_ti.xcom_push.assert_called_once_with(key='row_counts', value=unittest.mock.ANY)


# ---------------------------------------------------------------------------
# generate_summary
# ---------------------------------------------------------------------------

class TestGenerateSummary(unittest.TestCase):

    def test_returns_row_counts_from_xcom(self):
        row_counts = {
            'customer_segments': 10,
            'product_analysis': 5,
            'customer_rfm_analysis': 8,
            'product_cohorts': 4,
        }
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = row_counts

        result = analytics.generate_summary(task_instance=mock_ti)

        mock_ti.xcom_pull.assert_called_once_with(
            task_ids='data_quality_checks', key='row_counts'
        )
        self.assertEqual(result, row_counts)

    def test_logs_all_tables(self):
        row_counts = {'table_a': 1, 'table_b': 2}
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = row_counts

        with patch('packages.analytics_transforms.logging') as mock_log:
            analytics.generate_summary(task_instance=mock_ti)

        log_messages = [str(c) for c in mock_log.info.call_args_list]
        full_log = ' '.join(log_messages)
        self.assertIn('table_a', full_log)
        self.assertIn('table_b', full_log)


if __name__ == '__main__':
    unittest.main()

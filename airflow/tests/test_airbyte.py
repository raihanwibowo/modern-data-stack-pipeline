"""
Unit tests for packages/airbyte.py
Tests: get_auth, check_airbyte_health, trigger_airbyte_sync, check_airbyte_job_status
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

# Stub out airflow before importing the module under test
airflow_stub = MagicMock()
airflow_stub.exceptions.AirflowException = Exception
sys.modules.setdefault('airflow', airflow_stub)
sys.modules.setdefault('airflow.exceptions', airflow_stub.exceptions)

from packages.config import AirbyteConfig  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(
    url='http://airbyte:8000',
    username='airbyte',
    password='password',
    connection_id_1='conn-1',
    connection_id_2='conn-2',
):
    return AirbyteConfig(
        url=url,
        username=username,
        password=password,
        connection_id_1=connection_id_1,
        connection_id_2=connection_id_2,
    )


# ---------------------------------------------------------------------------
# get_auth
# ---------------------------------------------------------------------------

from dags.packages.airbyte import get_auth
class TestGetAuth(unittest.TestCase):

    def test_returns_tuple_when_credentials_present(self):
        config = _make_config(username='user', password='pass')
        with patch('packages.airbyte.get_airbyte_config', return_value=config):
            from packages.airbyte import get_auth
            result = get_auth()
        self.assertEqual(result, ('user', 'pass'))

    def test_returns_none_when_username_empty(self):
        config = _make_config(username='', password='')
        with patch('packages.airbyte.get_airbyte_config', return_value=config):
            from packages.airbyte import get_auth
            result = get_auth()
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# check_airbyte_health
# ---------------------------------------------------------------------------

class TestCheckAirbyteHealth(unittest.TestCase):

    def _import(self):
        from packages.airbyte import check_airbyte_health
        return check_airbyte_health

    def test_returns_true_on_healthy_response(self):
        config = _make_config(url='http://airbyte:8000')
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None

        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.requests.get', return_value=mock_response) as mock_get:
            result = self._import()()

        mock_get.assert_called_once_with('http://airbyte:8000/api/v1/health', timeout=10)
        self.assertTrue(result)

    def test_raises_airflow_exception_on_connection_error(self):
        import requests as req
        config = _make_config(url='http://airbyte:8000')

        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.requests.get', side_effect=req.exceptions.ConnectionError('refused')):
            with self.assertRaises(Exception) as ctx:
                self._import()()

        self.assertIn('not accessible', str(ctx.exception))

    def test_raises_airflow_exception_on_timeout(self):
        import requests as req
        config = _make_config()

        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.requests.get', side_effect=req.exceptions.Timeout('timeout')):
            with self.assertRaises(Exception):
                self._import()()


# ---------------------------------------------------------------------------
# trigger_airbyte_sync
# ---------------------------------------------------------------------------

class TestTriggerAirbyteSync(unittest.TestCase):

    def _import(self):
        from packages.airbyte import trigger_airbyte_sync
        return trigger_airbyte_sync

    def test_returns_job_id_on_success(self):
        config = _make_config(url='http://airbyte:8000')
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {'job': {'id': 42}}

        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.get_auth', return_value=('user', 'pass')), \
             patch('packages.airbyte.requests.post', return_value=mock_response):
            result = self._import()(connection_id='conn-abc')

        self.assertEqual(result, 42)

    def test_raises_when_connection_id_missing(self):
        with self.assertRaises(Exception) as ctx:
            self._import()(connection_id=None)
        self.assertIn('AIRBYTE_POSTGRES_TO_CLICKHOUSE_CONNECTION_ID', str(ctx.exception))

    def test_raises_when_job_id_missing_in_response(self):
        config = _make_config()
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {'job': {}}  # no 'id' key

        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.get_auth', return_value=None), \
             patch('packages.airbyte.requests.post', return_value=mock_response):
            with self.assertRaises(Exception) as ctx:
                self._import()(connection_id='conn-abc')
        self.assertIn('No job ID', str(ctx.exception))

    def test_raises_airflow_exception_on_http_error(self):
        import requests as req
        config = _make_config()

        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.get_auth', return_value=None), \
             patch('packages.airbyte.requests.post', side_effect=req.exceptions.HTTPError('500')):
            with self.assertRaises(Exception) as ctx:
                self._import()(connection_id='conn-abc')
        self.assertIn('Failed to trigger', str(ctx.exception))

    def test_posts_to_correct_url(self):
        config = _make_config(url='http://airbyte:8000')
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {'job': {'id': 99}}

        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.get_auth', return_value=None), \
             patch('packages.airbyte.requests.post', return_value=mock_response) as mock_post:
            self._import()(connection_id='my-conn')

        mock_post.assert_called_once_with(
            'http://airbyte:8000/api/v1/connections/sync',
            json={'connectionId': 'my-conn'},
            auth=None,
            timeout=30,
        )


# ---------------------------------------------------------------------------
# check_airbyte_job_status
# ---------------------------------------------------------------------------

class TestCheckAirbyteJobStatus(unittest.TestCase):

    def _import(self):
        from packages.airbyte import check_airbyte_job_status
        return check_airbyte_job_status

    def _mock_response(self, status, attempts=None):
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        job_data = {'job': {'status': status}}
        if attempts:
            job_data['job']['attempts'] = attempts
        mock_response.json.return_value = job_data
        return mock_response

    def test_raises_on_invalid_job_id_none(self):
        with self.assertRaises(Exception) as ctx:
            self._import()(job_id=None)
        self.assertIn('Invalid job ID', str(ctx.exception))

    def test_raises_on_invalid_job_id_string_none(self):
        with self.assertRaises(Exception) as ctx:
            self._import()(job_id='None')
        self.assertIn('Invalid job ID', str(ctx.exception))

    def test_returns_true_on_succeeded(self):
        config = _make_config()
        mock_response = self._mock_response(
            'succeeded',
            attempts=[{'recordsSynced': 1000, 'bytesSynced': 50000}]
        )
        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.get_auth', return_value=None), \
             patch('packages.airbyte.requests.post', return_value=mock_response):
            result = self._import()(job_id='123')
        self.assertTrue(result)

    def test_returns_false_on_running(self):
        config = _make_config()
        mock_response = self._mock_response('running')
        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.get_auth', return_value=None), \
             patch('packages.airbyte.requests.post', return_value=mock_response):
            result = self._import()(job_id='123')
        self.assertFalse(result)

    def test_returns_false_on_pending(self):
        config = _make_config()
        mock_response = self._mock_response('pending')
        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.get_auth', return_value=None), \
             patch('packages.airbyte.requests.post', return_value=mock_response):
            result = self._import()(job_id='123')
        self.assertFalse(result)

    def test_raises_on_failed_status(self):
        config = _make_config()
        mock_response = self._mock_response('failed')
        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.get_auth', return_value=None), \
             patch('packages.airbyte.requests.post', return_value=mock_response):
            with self.assertRaises(Exception) as ctx:
                self._import()(job_id='123')
        self.assertIn('failed', str(ctx.exception))

    def test_raises_on_cancelled_status(self):
        config = _make_config()
        mock_response = self._mock_response('cancelled')
        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.get_auth', return_value=None), \
             patch('packages.airbyte.requests.post', return_value=mock_response):
            with self.assertRaises(Exception) as ctx:
                self._import()(job_id='123')
        self.assertIn('cancelled', str(ctx.exception))

    def test_raises_on_unknown_status(self):
        config = _make_config()
        mock_response = self._mock_response('mystery_status')
        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.get_auth', return_value=None), \
             patch('packages.airbyte.requests.post', return_value=mock_response):
            with self.assertRaises(Exception) as ctx:
                self._import()(job_id='123')
        self.assertIn('Unknown job status', str(ctx.exception))

    def test_raises_on_request_exception(self):
        import requests as req
        config = _make_config()
        with patch('packages.airbyte.get_airbyte_config', return_value=config), \
             patch('packages.airbyte.get_auth', return_value=None), \
             patch('packages.airbyte.requests.post', side_effect=req.exceptions.Timeout('timed out')):
            with self.assertRaises(Exception) as ctx:
                self._import()(job_id='123')
        self.assertIn('Failed to check', str(ctx.exception))


if __name__ == '__main__':
    unittest.main()

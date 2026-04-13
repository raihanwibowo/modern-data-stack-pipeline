"""
Unit tests for packages/config.py
Tests AirbyteConfig and ClickHouseConfig dataclasses
"""
import os
import unittest
from unittest.mock import patch
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

from packages.config import AirbyteConfig, ClickHouseConfig


class TestAirbyteConfig(unittest.TestCase):

    def test_from_env_defaults(self):
        """AirbyteConfig.from_env() returns correct defaults when no env vars are set"""
        with patch.dict(os.environ, {}, clear=True):
            config = AirbyteConfig.from_env()

        self.assertEqual(config.url, 'http://host.docker.internal:8000')
        self.assertEqual(config.username, 'airbyte')
        self.assertEqual(config.password, 'password')
        self.assertEqual(config.connection_id_1, '')
        self.assertEqual(config.connection_id_2, '')

    def test_from_env_custom_values(self):
        """AirbyteConfig.from_env() picks up custom environment variables"""
        env = {
            'AIRBYTE_URL': 'http://custom-host:9000',
            'AIRBYTE_USERNAME': 'admin',
            'AIRBYTE_PASSWORD': 'secret',
            'AIRBYTE_CONNECTION_ID_1': 'conn-abc-123',
            'AIRBYTE_CONNECTION_ID_2': 'conn-def-456',
        }
        with patch.dict(os.environ, env):
            config = AirbyteConfig.from_env()

        self.assertEqual(config.url, 'http://custom-host:9000')
        self.assertEqual(config.username, 'admin')
        self.assertEqual(config.password, 'secret')
        self.assertEqual(config.connection_id_1, 'conn-abc-123')
        self.assertEqual(config.connection_id_2, 'conn-def-456')

    def test_dataclass_fields(self):
        """AirbyteConfig can be instantiated directly with positional args"""
        config = AirbyteConfig(
            url='http://localhost:8000',
            username='user',
            password='pass',
            connection_id_1='id1',
            connection_id_2='id2',
        )
        self.assertEqual(config.url, 'http://localhost:8000')
        self.assertEqual(config.connection_id_1, 'id1')
        self.assertEqual(config.connection_id_2, 'id2')

    def test_partial_env_override(self):
        """AirbyteConfig.from_env() uses defaults for missing env vars"""
        with patch.dict(os.environ, {'AIRBYTE_URL': 'http://override:1234'}, clear=True):
            config = AirbyteConfig.from_env()

        self.assertEqual(config.url, 'http://override:1234')
        self.assertEqual(config.username, 'airbyte')  # default
        self.assertEqual(config.password, 'password')  # default


class TestClickHouseConfig(unittest.TestCase):

    def test_from_env_defaults(self):
        """ClickHouseConfig.from_env() returns correct defaults when no env vars are set"""
        with patch.dict(os.environ, {}, clear=True):
            config = ClickHouseConfig.from_env()

        self.assertEqual(config.host, 'localhost')
        self.assertEqual(config.port, 9000)
        self.assertEqual(config.http_port, 8123)
        self.assertEqual(config.user, 'default')
        self.assertEqual(config.password, '')
        self.assertEqual(config.database, 'analytics')

    def test_from_env_custom_values(self):
        """ClickHouseConfig.from_env() picks up custom environment variables"""
        env = {
            'CLICKHOUSE_HOST': 'ch-server',
            'CLICKHOUSE_PORT': '19000',
            'CLICKHOUSE_HTTP_PORT': '18123',
            'CLICKHOUSE_USER': 'ch_user',
            'CLICKHOUSE_PASSWORD': 'ch_pass',
            'CLICKHOUSE_DATABASE': 'prod_analytics',
        }
        with patch.dict(os.environ, env):
            config = ClickHouseConfig.from_env()

        self.assertEqual(config.host, 'ch-server')
        self.assertEqual(config.port, 19000)
        self.assertEqual(config.http_port, 18123)
        self.assertEqual(config.user, 'ch_user')
        self.assertEqual(config.password, 'ch_pass')
        self.assertEqual(config.database, 'prod_analytics')

    def test_port_is_int(self):
        """ClickHouseConfig.from_env() casts port values to int"""
        with patch.dict(os.environ, {'CLICKHOUSE_PORT': '9001', 'CLICKHOUSE_HTTP_PORT': '8124'}):
            config = ClickHouseConfig.from_env()

        self.assertIsInstance(config.port, int)
        self.assertIsInstance(config.http_port, int)

    def test_dataclass_fields(self):
        """ClickHouseConfig can be instantiated directly"""
        config = ClickHouseConfig(
            host='localhost',
            port=9000,
            http_port=8123,
            user='default',
            password='',
            database='test_db',
        )
        self.assertEqual(config.database, 'test_db')
        self.assertEqual(config.port, 9000)


if __name__ == '__main__':
    unittest.main()

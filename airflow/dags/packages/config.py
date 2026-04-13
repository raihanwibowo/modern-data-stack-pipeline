import os
from dataclasses import dataclass

@dataclass
class AirbyteConfig:
    url: str
    username: str
    password: str
    connection_id_1: str
    connection_id_2: str

    @classmethod
    def from_env(cls):
        return cls(
            url=os.getenv('AIRBYTE_URL', 'http://host.docker.internal:8000'),
            username=os.getenv('AIRBYTE_USERNAME', 'airbyte'),
            password=os.getenv('AIRBYTE_PASSWORD', 'password'),
            connection_id_1=os.getenv('AIRBYTE_CONNECTION_ID_1', ''),
            connection_id_2=os.getenv('AIRBYTE_CONNECTION_ID_2', '')
        )

@dataclass
class ClickHouseConfig:
    host: str
    port: int
    http_port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls):
        return cls(
            host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
            port=int(os.getenv('CLICKHOUSE_PORT', 9000)),
            http_port=int(os.getenv('CLICKHOUSE_HTTP_PORT', 8123)),
            user=os.getenv('CLICKHOUSE_USER', 'default'),
            password=os.getenv('CLICKHOUSE_PASSWORD', ''),
            database=os.getenv('CLICKHOUSE_DATABASE', 'analytics')
        )
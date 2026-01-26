from typing import List, Any
from pydantic import BaseModel, Field


class BrokerConfig(BaseModel):
    host: str
    port: int
    client_id: str
    schema_registry_url: str
    topic_in: str
    topic_out: str
    linger_ms: int
    batch_size: int
    compression_type: str
    acks: int

class ClickhouseConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str
    secure: bool

class GoogleAIConfig(BaseModel):
    api_key: str

class ProxyConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str

class AppConfig(BaseModel):
    broker: BrokerConfig
    clickhouse: ClickhouseConfig
    google_ai: GoogleAIConfig
    proxy: ProxyConfig

from typing import Dict, Optional
from pydantic import BaseSettings, Field, BaseModel
from pathlib import Path
from functools import lru_cache


BASE_DIR = Path(__file__).resolve().parent.parent

class DBParams(BaseModel):
    host: str
    port: int
    user: str
    password: str
    database: str

class TenantDBConfig(BaseModel):
    prod: DBParams
    test: DBParams

class KafkaParams(BaseModel):
    bootstrap_servers: list
    input_topic: str
    output_topic: str

class TenantKafkaConfig(BaseModel):
    prod: KafkaParams
    test: KafkaParams

class MonitorConfig(BaseModel):
    recipients: Dict[str, str] = {}
    bad_request_recipients: Dict[str, str] = {}
    local_recipients: Dict[str, str] = {}

class Settings(BaseSettings):
    # 基本环境
    APP_ENV: str = Field("test", env="APP_ENV")   # e.g. "prod" or "test" or "dev"
    TENANT: str = Field("tongcheng_rk", env="TENANT")
    APP_NAME: str = "tongcheng_rkod"
    DEBUG: bool = True

    # Redis
    REDIS_URL: str = Field("redis://localhost:6379/0", env="REDIS_URL")

    # Model & storage
    MODEL_STORE_DIR: Path = Field(BASE_DIR / "models")

    # Logging
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    LOG_FILE: Optional[str] = Field("tongcheng_rkod_server.log", env="LOG_FILE")

    DB_CONFIG: Dict[str, TenantDBConfig] = Field(
        {
            "tongcheng_rk": {
                "prod": {
                    "host": "10.38.30.216",
                    "port": 19030,
                    "user": "gdmo_aid",
                    "password": "gdmo_aid@123!!",
                    "database": "iot",
                },
                "test": {
                    "host": "10.38.30.216",
                    "port": 19030,
                    "user": "gdmo_aid",
                    "password": "gdmo_aid@123!!",
                    "database": "iot",
                },
            }
        }
    )

    KAFKA_CONFIG: Dict[str, TenantKafkaConfig] = Field(
        {
            "tongcheng_rk": {
                "prod": {
                    "bootstrap_servers": ["10.38.30.210:9092", "10.38.30.211:9092", "10.38.30.212:9092"],
                    "input_topic": "iot_rkdh_process",
                    "output_topic": "iot_rkdh_process_result",
                },
                "test": {
                    "bootstrap_servers": ["10.38.30.210:9092", "10.38.30.211:9092", "10.38.30.212:9092"],
                    "input_topic": "iot_rkdh_process",
                    "output_topic": "iot_rkdh_process_result",
                },
            }
        }
    )

    MONITOR: MonitorConfig = Field(
        MonitorConfig(
            recipients={"prod": "caoxianfeng@gotion.com.cn", "test": "caoxianfeng@gotion.com.cn"},
            bad_request_recipients={"prod": "caoxianfeng@gotion.com.cn", "test": "caoxianfeng@gotion.com.cn"},
            local_recipients={"all": "caoxianfeng@gotion.com.cn", "per": "caoxianfeng@gotion.com.cn"},
        )
    )

    CREDENTIALS: Dict[str, tuple] = Field(
        {
            "prod": ("caoxianfeng@gotion.com.cn", "Ukky2EEMkz69iyit"),
            "test": ("caoxianfeng@gotion.com.cn", "Ukky2EEMkz69iyit"),
        }
    )

    MAIL_CONFIG: Dict = Field({"mail_host": ("smtp.exmail.qq.com", 25), "timeout": 2})

    class Config:
        env_file = str(BASE_DIR / ".env")
        env_file_encoding = "utf-8"

    def get_tenant_db(self, tenant_key: Optional[str] = None, env: Optional[str] = None) -> DBParams:
        tenant_key = tenant_key or self.TENANT
        env = env or self.APP_ENV
        try:
            entry = self.DB_CONFIG[tenant_key]
            if isinstance(entry, dict):
                entry = TenantDBConfig.parse_obj(entry)
            db_params = getattr(entry, env)
            return db_params.dict()
        except KeyError as e:
            raise KeyError(f"DB config not found for tenant={tenant_key} env={env}: {e}")

    def get_tenant_kafka(self, tenant_key: Optional[str] = None, env: Optional[str] = None) -> KafkaParams:
        tenant_key = tenant_key or self.TENANT
        env = env or self.APP_ENV
        try:
            entry = self.KAFKA_CONFIG[tenant_key]
            if isinstance(entry, dict):
                entry = TenantKafkaConfig.parse_obj(entry)
            kafka_params = getattr(entry, env)
            return kafka_params.dict()
        except KeyError as e:
            raise KeyError(f"Kafka config not found for tenant={tenant_key} env={env}: {e}")


@lru_cache()
def get_settings() -> Settings:
    return Settings()

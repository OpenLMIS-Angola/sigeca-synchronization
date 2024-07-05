from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List


@dataclass
class OpenLmisApiConfig:
    api_url: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    login_token: Optional[str] = None

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


@dataclass
class FallbackConfig:
    geographicZone: Optional[str] = None
    type: Optional[str] = None

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


@dataclass
class SigecaApiConfig:
    api_url: Optional[str] = None
    headers: Optional[Dict[str, str]] = field(default_factory=dict)
    credentials: Optional[Dict[str, str]] = field(default_factory=dict)
    skip_verification: Optional[bool] = None

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


@dataclass
class DatabaseConfig:
    username: Optional[str] = None
    password: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


@dataclass
class JdbcReaderConfig:
    jdbc_url: Optional[str] = None
    jdbc_user: Optional[str] = None
    jdbc_password: Optional[str] = None
    jdbc_driver: Optional[str] = None
    log_level: Optional[str] = None
    ssh_host: Optional[str] = None
    ssh_port: Optional[int] = None
    ssh_user: Optional[str] = None
    ssh_private_key_path: Optional[str] = None
    remote_bind_address: Optional[str] = None
    remote_bind_port: Optional[int] = None
    local_bind_port: Optional[int] = None

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


@dataclass
class SyncConfig:
    interval_minutes: Optional[int] = None
    synchronize_relevant: Optional[bool] = False
    email_report_list: Optional[List[str]] = field(default_factory=list)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


@dataclass
class SMTPClientConfig:
    server_url: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    sender: Optional[str] = None
    server_port: Optional[int] = 465

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


class SingletonMeta(type):
    """
    A Singleton metaclass that creates a single instance of a class.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


@dataclass
class Config(metaclass=SingletonMeta):
    open_lmis_api: Optional[OpenLmisApiConfig] = field(
        default_factory=OpenLmisApiConfig
    )
    sigeca_api: Optional[SigecaApiConfig] = field(default_factory=SigecaApiConfig)
    database: Optional[DatabaseConfig] = field(default_factory=DatabaseConfig)
    jdbc_reader: Optional[JdbcReaderConfig] = field(default_factory=JdbcReaderConfig)
    sync: Optional[SyncConfig] = field(default_factory=SyncConfig)
    additional_settings: Dict[str, Any] = field(default_factory=dict)
    fallbacks: Optional[FallbackConfig] = field(default_factory=FallbackConfig)
    smtp: Optional[SMTPClientConfig] = field(default_factory=SMTPClientConfig)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "Config":
        open_lmis_api = OpenLmisApiConfig(**config_dict.get("open_lmis_api", {}))
        sigeca_api = SigecaApiConfig(**config_dict.get("sigeca_api", {}))
        database = DatabaseConfig(**config_dict.get("database", {}))
        jdbc_reader = JdbcReaderConfig(**config_dict.get("jdbc_reader", {}))
        sync = SyncConfig(**config_dict.get("sync", {}))
        fallback = FallbackConfig(**config_dict.get("fallbacks", {}))
        smtp = SMTPClientConfig(**config_dict.get("smtp", {}))
        additional_settings = {
            k: v for k, v in config_dict.items() if k not in cls.__dataclass_fields__
        }

        instance = cls()
        instance.open_lmis_api = open_lmis_api
        instance.sigeca_api = sigeca_api
        instance.database = database
        instance.jdbc_reader = jdbc_reader
        instance.sync = sync
        instance.additional_settings = additional_settings
        instance.fallbacks = fallback
        instance.smtp = smtp

        return instance

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

import psycopg

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DbConfig:
    dsn: str


_db_config: DbConfig | None = None


def load_db_config() -> DbConfig:
    """
    Lê a DSN do Postgres via env var DATABASE_URL.
    Ex:
      postgresql://intuitive:intuitive@localhost:5432/intuitivecare
    """
    global _db_config
    if _db_config is None:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL não definido. Ex: postgresql://user:pass@host:5432/db")
        _db_config = DbConfig(dsn=dsn)
        logger.info("Configuração de banco de dados carregada")
    return _db_config


def get_conn() -> psycopg.Connection:
    """Abre uma conexão psycopg (context manager)."""
    try:
        cfg = load_db_config()
        return psycopg.connect(cfg.dsn)
    except psycopg.Error as e:
        logger.error(f"Erro ao conectar ao banco: {e}")
        raise
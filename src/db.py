from __future__ import annotations
import os
from urllib.parse import unquote, urlparse
import oracledb

def obter_url_do_banco() -> str:
    url = os.environ.get('DATABASE_URL', '').strip()
    if not url:
        return 'oracle://etl:etl_secret@localhost:1521/XEPDB1'
    return url

def _analisar_url_oracle(url: str) -> tuple[str, str, str]:
    u = urlparse(url)
    if u.scheme.lower() != 'oracle':
        raise ValueError('DATABASE_URL deve usar o esquema oracle:// (ex.: oracle://user:senha@host:1521/XEPDB1)')
    user = unquote(u.username or '')
    password = unquote(u.password or '')
    host = u.hostname or 'localhost'
    port = u.port or 1521
    service = (u.path or '/').lstrip('/')
    if not user or not service:
        raise ValueError('DATABASE_URL oracle:// requer usuário e nome do serviço (path).')
    dsn = oracledb.makedsn(host, port, service_name=service)
    return (user, password, dsn)

def conectar() -> oracledb.Connection:
    user, password, dsn = _analisar_url_oracle(obter_url_do_banco())
    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    return conn

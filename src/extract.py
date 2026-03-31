from __future__ import annotations
import re
import time
from pathlib import Path
import pandas as pd

def salvar_csv(df: pd.DataFrame, path: str | Path, **kwargs) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    defaults = {'index': False, 'encoding': 'utf-8-sig'}
    defaults.update(kwargs)
    df.to_csv(path, **defaults)
    return path

def carregar_csv(path: str | Path, *, retries: int=3, retry_delay_sec: float=0.5, **kwargs) -> pd.DataFrame:
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f'Arquivo não encontrado: {path}')
    merged = {'encoding': 'utf-8-sig'}
    merged.update({k: v for k, v in kwargs.items() if k not in ('retries', 'retry_delay_sec')})
    for attempt in range(max(1, retries)):
        try:
            enc = merged.get('encoding', 'utf-8-sig')
            header = pd.read_csv(path, nrows=0, encoding=enc)
            kwargs_read = dict(merged)
            if 'cpf' in header.columns:
                dtypes = dict(kwargs_read['dtype']) if isinstance(kwargs_read.get('dtype'), dict) else {}
                dtypes['cpf'] = 'string'
                kwargs_read['dtype'] = dtypes
            df = pd.read_csv(path, **kwargs_read)
            if 'cpf' in df.columns:
                df['cpf'] = _serie_cpf_como_texto(df['cpf'].astype('string'))
            return df
        except OSError:
            if attempt < retries - 1:
                time.sleep(retry_delay_sec)
            else:
                raise

def _serie_cpf_como_texto(s: pd.Series) -> pd.Series:

    def cell(v: object) -> str:
        if pd.isna(v):
            return ''
        if isinstance(v, float):
            if v == int(v) and abs(v) < 1000000000000.0:
                return str(int(v))
            t = str(v).strip()
            m = re.match('^(\\d+(?:\\.\\d+)?)e\\+(\\d+)$', t, re.I)
            if m:
                return str(int(float(t)))
            return t
        t = str(v).strip()
        if t in ('nan', '<NA>'):
            return ''
        m = re.match('^(\\d+)\\.0+$', t)
        if m:
            return m.group(1)
        return t
    return s.map(cell).astype(str)

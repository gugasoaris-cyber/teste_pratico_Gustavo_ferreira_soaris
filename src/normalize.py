from __future__ import annotations
import re
from datetime import datetime
import numpy as np
import pandas as pd
from src.transform import Source
_DATE_COLS: dict[Source, tuple[str, ...]] = {'rh': ('data_nascimento', 'data_admissao', 'data_saida'), 'esocial': ('data_nascimento', 'data_evento'), 'gestao': ('data_nascimento', 'data_admissao', 'data_saida')}
_CPF_COLUMN = 'cpf'

def normalizar_cpf_escalar(value: object) -> str | float:
    if pd.isna(value):
        return np.nan
    digits = re.sub('\\D', '', str(value).strip())
    if len(digits) == 11:
        return digits
    if len(digits) == 0:
        return np.nan
    return np.nan

def normalizar_cpf_serie(series: pd.Series) -> pd.Series:
    return series.map(normalizar_cpf_escalar)

def _analisar_datas(series: pd.Series) -> pd.Series:

    def one(val: object) -> pd.Timestamp:
        if pd.isna(val):
            return pd.NaT
        st = str(val).strip()
        if st in ('', 'nan'):
            return pd.NaT
        if re.match('^\\d{4}-\\d{2}-\\d{2}', st):
            return pd.to_datetime(st, errors='coerce')
        if re.match('^\\d{1,2}/\\d{1,2}/\\d{4}$', st):
            return pd.Timestamp(datetime.strptime(st, '%d/%m/%Y'))
        return pd.to_datetime(st, errors='coerce', dayfirst=True)
    ts = pd.to_datetime(series.map(one), errors='coerce')
    out = ts.dt.strftime('%Y-%m-%d')
    return out.where(ts.notna(), other=pd.NA)

def normalizar_numerico(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(',', '.', regex=False), errors='coerce')

def normalizar_formatos(df: pd.DataFrame, source: Source) -> pd.DataFrame:
    out = df.copy()
    if _CPF_COLUMN not in out.columns:
        raise ValueError("Coluna 'cpf' ausente.")
    out[_CPF_COLUMN] = normalizar_cpf_serie(out[_CPF_COLUMN])
    for col in _DATE_COLS.get(source, ()):
        if col in out.columns:
            out[col] = _analisar_datas(out[col])
    if 'salario_base' in out.columns:
        out['salario_base'] = normalizar_numerico(out['salario_base'])
    if source == 'gestao' and 'avaliacao_desempenho' in out.columns:
        out['avaliacao_desempenho'] = normalizar_numerico(out['avaliacao_desempenho'])
    if source == 'esocial' and 'codigo_categoria' in out.columns:

        def _cat_str(x: object) -> object:
            if pd.isna(x) or x == '':
                return pd.NA
            try:
                return str(int(float(x)))
            except (ValueError, TypeError):
                return str(x).strip()
        out['codigo_categoria'] = out['codigo_categoria'].map(_cat_str)
    for col in out.select_dtypes(include=['object']).columns:
        out[col] = out[col].replace('', pd.NA)
    return out

def remover_linhas_cpf_invalido(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if _CPF_COLUMN not in df.columns:
        return (df, 0)
    mask = df[_CPF_COLUMN].notna() & (df[_CPF_COLUMN].astype(str).str.len() == 11)
    removed = int((~mask).sum())
    return (df.loc[mask].reset_index(drop=True), removed)

def particionar_cpf_invalido(df: pd.DataFrame, fonte: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    if _CPF_COLUMN not in df.columns:
        return (df, pd.DataFrame())

    def motivo(val: object) -> str:
        if pd.isna(val) or val == '' or str(val).strip() in ('nan', '<NA>'):
            return 'cpf_ausente'
        d = re.sub('\\D', '', str(val).strip())
        if len(d) == 0:
            return 'cpf_ausente'
        if len(d) != 11:
            return 'cpf_quantidade_digitos_invalida'
        return 'cpf_invalido'
    mask = df[_CPF_COLUMN].notna() & (df[_CPF_COLUMN].astype(str).str.len() == 11)
    bad = df.loc[~mask].copy()
    if bad.empty:
        return (df.loc[mask].reset_index(drop=True), bad)
    bad['fonte'] = fonte
    bad['motivo_rejeicao'] = bad[_CPF_COLUMN].map(motivo)
    good = df.loc[mask].reset_index(drop=True)
    return (good, bad)

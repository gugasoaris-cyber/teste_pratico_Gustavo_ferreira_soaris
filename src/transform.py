from __future__ import annotations
from typing import Literal
import pandas as pd
Source = Literal['rh', 'esocial', 'gestao']
_RH_COLUMNS = ['nome', 'cpf', 'data_nascimento', 'sigla_orgao', 'data_admissao', 'data_saida', 'matricula', 'cargo', 'tipo_vinculo', 'salario_base', 'lotacao', 'status_servidor']
_ESOCIAL_COLUMNS = ['nome', 'cpf', 'data_nascimento', 'sigla_orgao', 'id_evento', 'tipo_evento', 'data_evento', 'codigo_categoria', 'indicador_retificacao', 'numero_recibo']
_GESTAO_RENAME: dict[str, str] = {'dataNascimento': 'data_nascimento', 'orgao': 'sigla_orgao', 'dataEntrada': 'data_admissao', 'dataSaida': 'data_saida', 'tipoVinculo': 'tipo_vinculo', 'salario': 'salario_base', 'unidadeLotacao': 'lotacao', 'status': 'status_servidor', 'estadoCivil': 'estado_civil', 'avaliacaoDesempenho': 'avaliacao_desempenho', 'cicloAvaliacao': 'ciclo_avaliacao'}

def padronizar_colunas(df: pd.DataFrame, source: Source) -> pd.DataFrame:
    out = df.copy()
    if source == 'gestao':
        missing = set(_GESTAO_RENAME.keys()) - set(out.columns)
        if missing:
            raise ValueError(f'Colunas esperadas ausentes na fonte gestao: {sorted(missing)}')
        out = out.rename(columns=_GESTAO_RENAME)
    elif source == 'rh':
        _verificar_colunas(out, _RH_COLUMNS, 'rh')
    elif source == 'esocial':
        _verificar_colunas(out, _ESOCIAL_COLUMNS, 'esocial')
    else:
        raise ValueError(f'Fonte desconhecida: {source}')
    _verificar_snake_case(out.columns, source)
    return out

def _verificar_colunas(df: pd.DataFrame, expected: list[str], source: str) -> None:
    missing = set(expected) - set(df.columns)
    extra = set(df.columns) - set(expected)
    if missing or extra:
        msg = []
        if missing:
            msg.append(f'faltando {sorted(missing)}')
        if extra:
            msg.append(f'extras {sorted(extra)}')
        raise ValueError(f"Schema inesperado em {source}: {', '.join(msg)}")

def _verificar_snake_case(columns: pd.Index, source: str) -> None:
    for col in columns:
        s = str(col)
        if s != s.lower():
            raise ValueError(f'Coluna não está em snake_case após padronização ({source}): {s!r}')
        if ' ' in s or '-' in s:
            raise ValueError(f'Coluna inválida após padronização ({source}): {s!r}')

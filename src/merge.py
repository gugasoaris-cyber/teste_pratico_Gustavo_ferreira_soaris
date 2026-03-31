from __future__ import annotations
import warnings
from pathlib import Path
import pandas as pd
PRIORIDADE_FONTE = ('RH', 'eSocial', 'Gestão')
MOTIVO_DUP_FONTE_1A = 'cpf_duplicado_mantida_primeira_linha'
MOTIVO_DUP_ESOCIAL_EVENTO = 'evento_descartado_mantido_data_evento_mais_recente'

def linhas_descartadas_manter_primeira(df: pd.DataFrame, subset: list[str] | None=None) -> pd.DataFrame:
    if df.empty or 'cpf' not in df.columns:
        return df.iloc[0:0].copy()
    sub = subset or ['cpf']
    return df[df.duplicated(subset=sub, keep='first')].copy()

def linhas_descartadas_esocial_ultimo_evento(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    if 'data_evento' not in df.columns:
        return linhas_descartadas_manter_primeira(df)
    parsed = pd.to_datetime(df['data_evento'], errors='coerce')
    tmp = df.assign(_ev=parsed).sort_values(['cpf', '_ev'], ascending=[True, False], na_position='last')
    dropped = tmp[tmp.duplicated(subset=['cpf'], keep='first')].drop(columns=['_ev'], errors='ignore')
    return dropped

def anotar_duplicados(df: pd.DataFrame, fonte: str, motivo: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=['fonte', 'motivo_duplicidade'])
    out = df.copy()
    out.insert(0, 'motivo_duplicidade', motivo)
    out.insert(0, 'fonte', fonte)
    return out

def deduplicar_fonte_manter_primeira(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    before = len(df)
    out = df.drop_duplicates(subset=['cpf'], keep='first').reset_index(drop=True)
    return (out, before - len(out))

def deduplicar_esocial_ultimo_evento(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or 'data_evento' not in df.columns:
        return df.drop_duplicates(subset=['cpf'], keep='first').reset_index(drop=True)
    parsed = pd.to_datetime(df['data_evento'], errors='coerce')
    tmp = df.assign(_ev=parsed).sort_values(['cpf', '_ev'], ascending=[True, False], na_position='last')
    return tmp.drop_duplicates(subset=['cpf'], keep='first').drop(columns=['_ev']).reset_index(drop=True)

def mesclar_por_prioridade(rh: pd.DataFrame, esocial: pd.DataFrame, gestao: pd.DataFrame) -> pd.DataFrame:
    r = rh.set_index('cpf')
    e = esocial.set_index('cpf')
    g = gestao.set_index('cpf')
    all_idx = r.index.union(e.index).union(g.index)
    all_cols = sorted(set(r.columns) | set(e.columns) | set(g.columns))
    series_list: dict[str, pd.Series] = {}
    for col in all_cols:
        sr = r[col] if col in r.columns else pd.Series(dtype=object, index=r.index)
        se = e[col] if col in e.columns else pd.Series(dtype=object, index=e.index)
        sg = g[col] if col in g.columns else pd.Series(dtype=object, index=g.index)
        a = sr.reindex(all_idx).astype(object)
        b = se.reindex(all_idx).astype(object)
        c = sg.reindex(all_idx).astype(object)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', FutureWarning)
            merged = a.fillna(b).fillna(c)
        series_list[col] = merged
    out = pd.DataFrame(series_list, index=all_idx)
    out.index.name = 'cpf'
    out = out.reset_index()
    cols = _colunas_ordenadas(list(out.columns))
    return out.reindex(columns=cols)

def _colunas_ordenadas(cols: list[str]) -> list[str]:
    first = ['cpf', 'nome', 'data_nascimento', 'sigla_orgao', 'data_admissao', 'data_saida', 'matricula', 'cargo', 'tipo_vinculo', 'salario_base', 'lotacao', 'status_servidor', 'sexo', 'estado_civil', 'cor', 'nacionalidade', 'email', 'telefone', 'avaliacao_desempenho', 'ciclo_avaliacao', 'id_evento', 'tipo_evento', 'data_evento', 'codigo_categoria', 'indicador_retificacao', 'numero_recibo']
    present = [c for c in first if c in cols]
    rest = sorted((c for c in cols if c not in present))
    return present + rest

def relatorio_duplicidade(rh: pd.DataFrame, esocial: pd.DataFrame, gestao: pd.DataFrame, esocial_before_dedupe: pd.DataFrame | None=None, consolidado: pd.DataFrame | None=None) -> str:

    def dup_mask(s: pd.Series) -> int:
        return int(s.duplicated(keep=False).sum())
    lines = ['=== Relatório de duplicidade (CPF) ===', '', f"Prioridade de preenchimento no consolidado: {' > '.join(PRIORIDADE_FONTE)}", '', '[RH]', f'  Linhas: {len(rh):,}', f"  CPFs únicos: {rh['cpf'].nunique():,}", f"  Linhas com CPF repetido na fonte: {dup_mask(rh['cpf']):,}", '', '[eSocial]', f'  Linhas: {len(esocial):,}', f"  CPFs únicos: {esocial['cpf'].nunique():,}", f"  Linhas com CPF repetido na fonte (após 1 linha/CPF p/ merge): {dup_mask(esocial['cpf']):,}"]
    if esocial_before_dedupe is not None:
        n_lin = len(esocial_before_dedupe)
        n_cpfs = esocial_before_dedupe['cpf'].nunique()
        mult = n_lin - n_cpfs
        lines.extend([f'  (Antes do desdupe por evento: {n_lin:,} linhas, {n_cpfs:,} CPFs únicos, ~{mult:,} linhas extras por múltiplos eventos)'])
    lines.extend(['', '[Gestão]', f'  Linhas: {len(gestao):,}', f"  CPFs únicos: {gestao['cpf'].nunique():,}", f"  Linhas com CPF repetido na fonte: {dup_mask(gestao['cpf']):,}", ''])
    u_rh = set(rh['cpf'].dropna().unique())
    u_es = set(esocial['cpf'].dropna().unique())
    u_ge = set(gestao['cpf'].dropna().unique())
    lines.append(f'[Cruzamento] CPFs em qualquer fonte: {len(u_rh | u_es | u_ge):,}')
    lines.append(f'  Só RH: {len(u_rh - u_es - u_ge):,}')
    lines.append(f'  Só eSocial: {len(u_es - u_rh - u_ge):,}')
    lines.append(f'  Só Gestão: {len(u_ge - u_rh - u_es):,}')
    lines.append('')
    if consolidado is not None:
        dup = int(consolidado['cpf'].duplicated().sum())
        lines.extend(['[Consolidado]', f'  Linhas: {len(consolidado):,}', f"  CPFs únicos: {consolidado['cpf'].nunique():,}", f'  Linhas com CPF duplicado (esperado 0): {dup:,}', ''])
    return '\n'.join(lines) + '\n'

def salvar_relatorio(text: str, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')
    return path

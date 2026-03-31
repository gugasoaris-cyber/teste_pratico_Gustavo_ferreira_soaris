from __future__ import annotations
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from pipelines import etl_esocial, etl_gestao, etl_rh
from src.config import DATA_DIR, LOGS_DIR, PROCESSED_DIR
from src.extract import carregar_csv, salvar_csv
from src.merge import MOTIVO_DUP_ESOCIAL_EVENTO, MOTIVO_DUP_FONTE_1A, anotar_duplicados, deduplicar_esocial_ultimo_evento, deduplicar_fonte_manter_primeira, linhas_descartadas_esocial_ultimo_evento, linhas_descartadas_manter_primeira, mesclar_por_prioridade, relatorio_duplicidade, salvar_relatorio
from src.metrics import ExecutionMetrics
from src.normalize import normalizar_formatos, particionar_cpf_invalido
from src.pipeline_resume import CHECKPOINT_DIR, CP_CONSOLIDADO, CP_EXTRACT_ESOCIAL, CP_EXTRACT_GESTAO, CP_EXTRACT_RH, CP_REJ_ES, CP_REJ_GE, CP_REJ_RH, CP_DUP_ESOCIAL, CP_DUP_GESTAO, CP_DUP_RH, CP_TR_ESOCIAL, CP_TR_ESOCIAL_PRE, CP_TR_GESTAO, CP_TR_RH, atualizar_meta, carregar_estado, carregar_meta, limpar_arquivos_retomada, pode_pular_etapa, salvar_progresso, validar_estado_execucao

def _salvar_checkpoint_rejeitados(df: pd.DataFrame, filename: str) -> None:
    path = CHECKPOINT_DIR / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        pd.DataFrame(columns=['fonte', 'motivo_rejeicao']).to_csv(path, index=False, encoding='utf-8-sig')
    else:
        salvar_csv(df, path)

def _carregar_checkpoint_rejeitados(filename: str) -> pd.DataFrame:
    path = CHECKPOINT_DIR / filename
    if not path.is_file():
        return pd.DataFrame(columns=['fonte', 'motivo_rejeicao'])
    return carregar_csv(path)

def _salvar_checkpoint_duplicados(df: pd.DataFrame, filename: str) -> None:
    path = CHECKPOINT_DIR / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        pd.DataFrame(columns=['fonte', 'motivo_duplicidade']).to_csv(path, index=False, encoding='utf-8-sig')
    else:
        salvar_csv(df, path)

def _carregar_checkpoint_duplicados(filename: str) -> pd.DataFrame:
    path = CHECKPOINT_DIR / filename
    if not path.is_file():
        return pd.DataFrame(columns=['fonte', 'motivo_duplicidade'])
    return carregar_csv(path)

def principal() -> None:
    data_dir = DATA_DIR
    parser = argparse.ArgumentParser(description='ETL Pessoas — padronização, merge e verificação de duplicidade (CPF)')
    parser.add_argument('--sample', type=int, default=None, metavar='N', help='Lê apenas N linhas de cada CSV (útil para desenvolvimento)')
    parser.add_argument('--resume', action='store_true', help='Retoma a partir da última etapa gravada em data/processed/_checkpoint/')
    parser.add_argument('--clear-checkpoint', action='store_true', help='Remove checkpoint e estado de retomada e segue execução normal (pipeline completo)')
    args = parser.parse_args()
    read_kw = {}
    if args.sample is not None:
        read_kw['nrows'] = args.sample
    if args.clear_checkpoint:
        limpar_arquivos_retomada()
        print('Checkpoint e estado de retomada removidos.\n')
    use_resume = args.resume and (not args.clear_checkpoint)
    state = carregar_estado(use_resume)
    last_completed = validar_estado_execucao(state, args.sample)
    if use_resume and state is None:
        print('Aviso: --resume sem checkpoint válido; executando pipeline completo.\n')
    metrics = ExecutionMetrics()
    meta = carregar_meta() if use_resume and last_completed else {'drop_dup_rh': 0, 'drop_dup_ge': 0}
    t0 = time.perf_counter()
    if pode_pular_etapa(last_completed, 'extract_rh'):
        rh = carregar_csv(CHECKPOINT_DIR / CP_EXTRACT_RH)
    else:
        rh = etl_rh.executar(data_dir, **read_kw)
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        salvar_csv(rh, CHECKPOINT_DIR / CP_EXTRACT_RH)
        salvar_progresso(args.sample, 'extract_rh')
    metrics.rows_read_rh = len(rh)
    metrics.registrar_etapa('extract_rh', t0)
    t0 = time.perf_counter()
    if pode_pular_etapa(last_completed, 'extract_esocial'):
        esocial = carregar_csv(CHECKPOINT_DIR / CP_EXTRACT_ESOCIAL)
    else:
        esocial = etl_esocial.executar(data_dir, **read_kw)
        salvar_csv(esocial, CHECKPOINT_DIR / CP_EXTRACT_ESOCIAL)
        salvar_progresso(args.sample, 'extract_esocial')
    metrics.rows_read_esocial = len(esocial)
    metrics.registrar_etapa('extract_esocial', t0)
    t0 = time.perf_counter()
    if pode_pular_etapa(last_completed, 'extract_gestao'):
        gestao = carregar_csv(CHECKPOINT_DIR / CP_EXTRACT_GESTAO)
    else:
        gestao = etl_gestao.executar(data_dir, **read_kw)
        salvar_csv(gestao, CHECKPOINT_DIR / CP_EXTRACT_GESTAO)
        salvar_progresso(args.sample, 'extract_gestao')
    metrics.rows_read_gestao = len(gestao)
    metrics.registrar_etapa('extract_gestao', t0)
    t0 = time.perf_counter()
    if pode_pular_etapa(last_completed, 'transform_rh'):
        rh = carregar_csv(CHECKPOINT_DIR / CP_TR_RH)
        rej_rh = _carregar_checkpoint_rejeitados(CP_REJ_RH)
        dup_rh = _carregar_checkpoint_duplicados(CP_DUP_RH)
        drop_dup_rh = meta['drop_dup_rh']
    else:
        rh = normalizar_formatos(rh, 'rh')
        rh, rej_rh = particionar_cpf_invalido(rh, 'rh')
        rh_pre_dedupe = rh.copy()
        dup_rh = anotar_duplicados(linhas_descartadas_manter_primeira(rh_pre_dedupe), 'rh', MOTIVO_DUP_FONTE_1A)
        rh, drop_dup_rh = deduplicar_fonte_manter_primeira(rh)
        salvar_csv(rh, CHECKPOINT_DIR / CP_TR_RH)
        _salvar_checkpoint_rejeitados(rej_rh, CP_REJ_RH)
        _salvar_checkpoint_duplicados(dup_rh, CP_DUP_RH)
        atualizar_meta(drop_dup_rh=drop_dup_rh)
        salvar_progresso(args.sample, 'transform_rh')
    metrics.rows_accepted_rh = len(rh)
    metrics.registrar_etapa('transform_rh', t0)
    t0 = time.perf_counter()
    if pode_pular_etapa(last_completed, 'transform_esocial'):
        esocial = carregar_csv(CHECKPOINT_DIR / CP_TR_ESOCIAL)
        esocial_pre_event_dedupe = carregar_csv(CHECKPOINT_DIR / CP_TR_ESOCIAL_PRE)
        rej_es = _carregar_checkpoint_rejeitados(CP_REJ_ES)
        dup_es = _carregar_checkpoint_duplicados(CP_DUP_ESOCIAL)
    else:
        esocial = normalizar_formatos(esocial, 'esocial')
        esocial, rej_es = particionar_cpf_invalido(esocial, 'esocial')
        esocial_pre_event_dedupe = esocial.copy()
        dup_es = anotar_duplicados(linhas_descartadas_esocial_ultimo_evento(esocial_pre_event_dedupe), 'esocial', MOTIVO_DUP_ESOCIAL_EVENTO)
        esocial = deduplicar_esocial_ultimo_evento(esocial)
        salvar_csv(esocial, CHECKPOINT_DIR / CP_TR_ESOCIAL)
        salvar_csv(esocial_pre_event_dedupe, CHECKPOINT_DIR / CP_TR_ESOCIAL_PRE)
        _salvar_checkpoint_rejeitados(rej_es, CP_REJ_ES)
        _salvar_checkpoint_duplicados(dup_es, CP_DUP_ESOCIAL)
        salvar_progresso(args.sample, 'transform_esocial')
    metrics.rows_accepted_esocial = len(esocial)
    metrics.registrar_etapa('transform_esocial', t0)
    t0 = time.perf_counter()
    if pode_pular_etapa(last_completed, 'transform_gestao'):
        gestao = carregar_csv(CHECKPOINT_DIR / CP_TR_GESTAO)
        rej_ge = _carregar_checkpoint_rejeitados(CP_REJ_GE)
        dup_ge = _carregar_checkpoint_duplicados(CP_DUP_GESTAO)
        drop_dup_ge = meta['drop_dup_ge']
    else:
        gestao = normalizar_formatos(gestao, 'gestao')
        gestao, rej_ge = particionar_cpf_invalido(gestao, 'gestao')
        gestao_pre_dedupe = gestao.copy()
        dup_ge = anotar_duplicados(linhas_descartadas_manter_primeira(gestao_pre_dedupe), 'gestao', MOTIVO_DUP_FONTE_1A)
        gestao, drop_dup_ge = deduplicar_fonte_manter_primeira(gestao)
        salvar_csv(gestao, CHECKPOINT_DIR / CP_TR_GESTAO)
        _salvar_checkpoint_rejeitados(rej_ge, CP_REJ_GE)
        _salvar_checkpoint_duplicados(dup_ge, CP_DUP_GESTAO)
        atualizar_meta(drop_dup_ge=drop_dup_ge)
        salvar_progresso(args.sample, 'transform_gestao')
    metrics.rows_accepted_gestao = len(gestao)
    metrics.registrar_etapa('transform_gestao', t0)
    metrics.rows_rejected_total = len(rej_rh) + len(rej_es) + len(rej_ge)
    t0 = time.perf_counter()
    if pode_pular_etapa(last_completed, 'merge'):
        consolidado = carregar_csv(CHECKPOINT_DIR / CP_CONSOLIDADO)
    else:
        consolidado = mesclar_por_prioridade(rh, esocial, gestao)
        salvar_csv(consolidado, CHECKPOINT_DIR / CP_CONSOLIDADO)
        salvar_progresso(args.sample, 'merge')
    metrics.rows_consolidado = len(consolidado)
    metrics.registrar_etapa('merge', t0)
    t0 = time.perf_counter()
    out_rh = salvar_csv(rh, PROCESSED_DIR / 'rh.csv')
    out_esocial = salvar_csv(esocial, PROCESSED_DIR / 'esocial.csv')
    out_gestao = salvar_csv(gestao, PROCESSED_DIR / 'gestao.csv')
    out_con = salvar_csv(consolidado, PROCESSED_DIR / 'pessoas_consolidado.csv')
    metrics.registrar_etapa('persist', t0)
    metrics_path = metrics.salvar_json(LOGS_DIR / 'etl_metrics.json')
    rel = relatorio_duplicidade(rh, esocial, gestao, esocial_before_dedupe=esocial_pre_event_dedupe, consolidado=consolidado)
    rel_path = salvar_relatorio(rel, LOGS_DIR / 'duplicidade_cpf.txt')
    rej_parts = [r for r in (rej_rh, rej_es, rej_ge) if not r.empty]
    rej_path: Path | None = None
    if rej_parts:
        rej_path = salvar_csv(pd.concat(rej_parts, ignore_index=True), LOGS_DIR / 'rejeitados.csv')
    dup_parts = [d for d in (dup_rh, dup_es, dup_ge) if not d.empty]
    dup_path: Path | None = None
    if dup_parts:
        dup_path = salvar_csv(pd.concat(dup_parts, ignore_index=True), LOGS_DIR / 'duplicados.csv')
    checkpoint = {'completed_at': datetime.now(timezone.utc).isoformat(), 'rows_consolidado': metrics.rows_consolidado, 'rows_rejected_total': metrics.rows_rejected_total, 'outputs': {'rh': str(out_rh), 'esocial': str(out_esocial), 'gestao': str(out_gestao), 'consolidado': str(out_con), 'metrics': str(metrics_path), 'duplicidade': str(rel_path), 'rejeitados': str(rej_path) if rej_path else None, 'duplicados': str(dup_path) if dup_path else None}}
    (LOGS_DIR / 'etl_checkpoint.json').write_text(json.dumps(checkpoint, ensure_ascii=False, indent=2), encoding='utf-8')
    limpar_arquivos_retomada()
    print('Processamento concluído.\n')
    print('Fontes tratadas (originais em data/ não foram alterados):')
    print(f'  {out_rh}')
    print(f'  {out_esocial}')
    print(f'  {out_gestao}')
    print(f'  {out_con}\n')
    print(f'Relatório de duplicidade: {rel_path}')
    print(f'Métricas (JSON): {metrics_path}')
    if rej_path:
        print(f'Linhas rejeitadas (CPF inválido): {rej_path}')
    if dup_path:
        print(f'Linhas duplicadas descartadas (auditoria): {dup_path}')
    print()
    print(f'Linhas rejeitadas por CPF inválido — RH: {len(rej_rh)}, eSocial: {len(rej_es)}, Gestão: {len(rej_ge)}')
    print(f'Linhas deduplicadas (mesmo CPF, mantida 1ª) — RH: {drop_dup_rh}, Gestão: {drop_dup_ge}\n')
    print(f'Consolidado: {len(consolidado):,} linhas × {consolidado.shape[1]} colunas')
    dup_cons = int(consolidado['cpf'].duplicated().sum())
    if dup_cons:
        print(f'AVISO: CPFs duplicados no consolidado: {dup_cons}')
    else:
        print('CPF único por linha no consolidado: OK.')
    print('\n--- Relatório (trecho) ---')
    print('\n'.join(rel.strip().splitlines()[:18]))
    print('...')
if __name__ == '__main__':
    principal()

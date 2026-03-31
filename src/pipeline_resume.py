from __future__ import annotations
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from src.config import LOGS_DIR, PROCESSED_DIR
CHECKPOINT_DIR = PROCESSED_DIR / '_checkpoint'
STATE_PATH = LOGS_DIR / 'etl_pipeline_state.json'
STAGES = ['extract_rh', 'extract_esocial', 'extract_gestao', 'transform_rh', 'transform_esocial', 'transform_gestao', 'merge']

def caminho_checkpoint(name: str) -> Path:
    return CHECKPOINT_DIR / name

def indice_etapa(name: str) -> int:
    return STAGES.index(name)

def pode_pular_etapa(last_completed: str | None, stage: str) -> bool:
    if not last_completed:
        return False
    return indice_etapa(last_completed) >= indice_etapa(stage)

def carregar_estado(resume: bool) -> dict[str, Any] | None:
    if not resume or not STATE_PATH.is_file():
        return None
    return json.loads(STATE_PATH.read_text(encoding='utf-8'))

def validar_estado_execucao(state: dict[str, Any] | None, sample: int | None) -> str | None:
    if state is None:
        return None
    if state.get('sample') != sample:
        raise SystemExit(f"Checkpoint incompatível: use o mesmo --sample da execução anterior (checkpoint: {state.get('sample')!r}, atual: {sample!r}). Apague logs/etl_pipeline_state.json e data/processed/_checkpoint/ para recomeçar.")
    return state.get('last_completed')

def salvar_progresso(sample: int | None, last_completed: str) -> None:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {'version': 1, 'sample': sample, 'last_completed': last_completed, 'updated_at': datetime.now(timezone.utc).isoformat()}
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

def limpar_arquivos_retomada() -> None:
    STATE_PATH.unlink(missing_ok=True)
    if CHECKPOINT_DIR.is_dir():
        shutil.rmtree(CHECKPOINT_DIR, ignore_errors=True)
CP_EXTRACT_RH = 'extract_rh.csv'
CP_EXTRACT_ESOCIAL = 'extract_esocial.csv'
CP_EXTRACT_GESTAO = 'extract_gestao.csv'
CP_TR_RH = 'tr_rh.csv'
CP_REJ_RH = 'rej_rh.csv'
CP_TR_ESOCIAL = 'tr_esocial.csv'
CP_TR_ESOCIAL_PRE = 'tr_esocial_pre.csv'
CP_REJ_ES = 'rej_es.csv'
CP_TR_GESTAO = 'tr_gestao.csv'
CP_REJ_GE = 'rej_ge.csv'
CP_DUP_RH = 'dup_rh.csv'
CP_DUP_ESOCIAL = 'dup_esocial.csv'
CP_DUP_GESTAO = 'dup_gestao.csv'
CP_CONSOLIDADO = 'consolidado.csv'
META_PATH = CHECKPOINT_DIR / 'meta.json'

def carregar_meta() -> dict[str, int]:
    if not META_PATH.is_file():
        return {'drop_dup_rh': 0, 'drop_dup_ge': 0}
    data = json.loads(META_PATH.read_text(encoding='utf-8'))
    return {'drop_dup_rh': int(data.get('drop_dup_rh', 0)), 'drop_dup_ge': int(data.get('drop_dup_ge', 0))}

def atualizar_meta(**kwargs: int) -> None:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    m = carregar_meta()
    m.update(kwargs)
    META_PATH.write_text(json.dumps(m, ensure_ascii=False, indent=2), encoding='utf-8')

from __future__ import annotations
import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
import oracledb
import pandas as pd
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from src.config import PROCESSED_DIR
from src.db import conectar, obter_url_do_banco
LOAD_STATE_PATH = ROOT / 'logs' / 'oracle_load_state.json'
PERSON_PHASE = 'pessoas'
EVENT_PHASE = 'eventos'

def _valor_ausente(x: object) -> bool:
    if x == '':
        return True
    try:
        return bool(pd.isna(x))
    except (ValueError, TypeError):
        return False

def _para_data(val: object) -> date | None:
    if _valor_ausente(val):
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if hasattr(val, 'date') and callable(getattr(val, 'date', None)):
        try:
            d = val.date()
            if isinstance(d, datetime):
                return d.date()
            return d
        except (ValueError, OSError, AttributeError):
            pass
    s = str(val).strip()[:10]
    if not s:
        return None
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None

def _para_float(val: object):
    if _valor_ausente(val):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def _normalizar_cpf(val: object) -> str | None:
    if _valor_ausente(val):
        return None
    d = ''.join((c for c in str(val) if c.isdigit()))
    if len(d) == 11:
        return d
    return None

def _esvaziar_tabelas_carga(cur: oracledb.Cursor) -> None:
    for tbl in ('avaliacao_desempenho', 'evento', 'vinculo', 'pessoa', 'cargo', 'orgao'):
        cur.execute(f'TRUNCATE TABLE etl.{tbl}')

def _carregar_mapas_catalogo(cur: oracledb.Cursor):
    cur.execute('SELECT id, codigo FROM etl.tipo_vinculo')
    tv = {r[1]: r[0] for r in cur.fetchall()}
    cur.execute('SELECT id, codigo FROM etl.status_servidor')
    st = {r[1]: r[0] for r in cur.fetchall()}
    cur.execute('SELECT id, codigo FROM etl.tipo_evento')
    te = {r[1]: r[0] for r in cur.fetchall()}
    return (tv, st, te)

def _carregar_estado_carga() -> dict:
    if not LOAD_STATE_PATH.is_file():
        return {}
    return json.loads(LOAD_STATE_PATH.read_text(encoding='utf-8'))

def _salvar_estado_carga(state: dict) -> None:
    LOAD_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOAD_STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding='utf-8')

def _limpar_estado_carga() -> None:
    LOAD_STATE_PATH.unlink(missing_ok=True)

def _validar_estado_carga(state: dict, sample: int | None, no_truncate: bool) -> None:
    if not state:
        return
    if state.get('sample') != sample:
        raise SystemExit(
            f"Checkpoint de carga incompatível para --sample (checkpoint: {state.get('sample')!r}, atual: {sample!r}). "
            "Use --clear-load-checkpoint para reiniciar a carga."
        )
    if bool(state.get('no_truncate')) != bool(no_truncate):
        raise SystemExit(
            f"Checkpoint de carga incompatível para --no-truncate (checkpoint: {state.get('no_truncate')!r}, atual: {no_truncate!r}). "
            "Use --clear-load-checkpoint para reiniciar a carga."
        )

def _garantir_orgaos_cargos(cur: oracledb.Cursor, df_con: pd.DataFrame) -> tuple[dict[str, int], dict[str, int]]:
    for s in df_con['sigla_orgao'].dropna().astype(str).unique():
        cur.execute('\n            INSERT INTO etl.orgao (sigla)\n            SELECT :1 FROM DUAL\n            WHERE NOT EXISTS (SELECT 1 FROM etl.orgao WHERE sigla = :2)\n            ', [s, s])
    for c in df_con['cargo'].dropna().astype(str).unique():
        cur.execute('\n            INSERT INTO etl.cargo (nome)\n            SELECT :1 FROM DUAL\n            WHERE NOT EXISTS (SELECT 1 FROM etl.cargo WHERE nome = :2)\n            ', [c, c])
    cur.execute('SELECT id, sigla FROM etl.orgao')
    org_map = {r[1]: r[0] for r in cur.fetchall()}
    cur.execute('SELECT id, nome FROM etl.cargo')
    car_map = {r[1]: r[0] for r in cur.fetchall()}
    return (org_map, car_map)

def carregar_consolidado(cur: oracledb.Cursor, df: pd.DataFrame, org_map: dict[str, int], car_map: dict[str, int], tv_map: dict[str, int], st_map: dict[str, int]) -> dict[str, int]:
    cpf_to_id: dict[str, int] = {}
    for _, row in df.iterrows():
        r = row.to_dict()
        cpf = _normalizar_cpf(r.get('cpf'))
        if not cpf:
            continue
        nome = r.get('nome')
        if _valor_ausente(nome):
            nome = 'NOME NÃO INFORMADO'
        id_var = cur.var(int)
        cur.execute('\n            INSERT INTO etl.pessoa (cpf, nome, data_nascimento, sexo, estado_civil, cor_raca, nacionalidade, email, telefone)\n            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)\n            RETURNING id INTO :10\n            ', [cpf, str(nome)[:512], _para_data(r.get('data_nascimento')), None if _valor_ausente(r.get('sexo')) else str(r['sexo'])[:20], None if _valor_ausente(r.get('estado_civil')) else str(r['estado_civil'])[:64], None if _valor_ausente(r.get('cor')) else str(r['cor'])[:32], None if _valor_ausente(r.get('nacionalidade')) else str(r['nacionalidade'])[:64], None if _valor_ausente(r.get('email')) else str(r['email'])[:256], None if _valor_ausente(r.get('telefone')) else str(r['telefone'])[:64], id_var])
        pid = int(id_var.getvalue()[0])
        cpf_to_id[cpf] = pid
        sigla = r.get('sigla_orgao')
        cargo_n = r.get('cargo')
        if _valor_ausente(sigla) or str(sigla) not in org_map:
            raise ValueError(f'Órgão inválido para CPF {cpf}: {sigla!r}')
        if _valor_ausente(cargo_n) or str(cargo_n) not in car_map:
            raise ValueError(f'Cargo inválido para CPF {cpf}: {cargo_n!r}')
        tv = r.get('tipo_vinculo')
        st = r.get('status_servidor')
        if _valor_ausente(tv) or str(tv) not in tv_map:
            raise ValueError(f'tipo_vinculo inválido para CPF {cpf}: {tv!r}')
        if _valor_ausente(st) or str(st) not in st_map:
            raise ValueError(f'status_servidor inválido para CPF {cpf}: {st!r}')
        cur.execute('\n            INSERT INTO etl.vinculo (\n                pessoa_id, orgao_id, cargo_id, tipo_vinculo_id, status_servidor_id,\n                matricula, salario_base, lotacao, data_admissao, data_saida\n            ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)\n            ', (pid, org_map[str(sigla)], car_map[str(cargo_n)], tv_map[str(tv)], st_map[str(st)], None if _valor_ausente(r.get('matricula')) else str(r['matricula'])[:128], _para_float(r.get('salario_base')), None if _valor_ausente(r.get('lotacao')) else str(r['lotacao'])[:4000], _para_data(r.get('data_admissao')), _para_data(r.get('data_saida'))))
        nota = _para_float(r.get('avaliacao_desempenho'))
        ciclo = r.get('ciclo_avaliacao')
        if not _valor_ausente(ciclo) and nota is not None:
            cur.execute('\n                MERGE INTO etl.avaliacao_desempenho d\n                USING (SELECT :1 AS pessoa_id, :2 AS ciclo, :3 AS nota FROM DUAL) s\n                ON (d.pessoa_id = s.pessoa_id AND d.ciclo_avaliacao = s.ciclo)\n                WHEN MATCHED THEN UPDATE SET d.nota = s.nota\n                WHEN NOT MATCHED THEN INSERT (pessoa_id, ciclo_avaliacao, nota)\n                VALUES (s.pessoa_id, s.ciclo, s.nota)\n                ', (pid, str(ciclo)[:16], nota))
    return cpf_to_id

def _upsert_pessoa(cur: oracledb.Cursor, r: dict) -> tuple[str | None, int | None]:
    cpf = _normalizar_cpf(r.get('cpf'))
    if not cpf:
        return (None, None)
    cur.execute('SELECT id FROM etl.pessoa WHERE cpf = :1', [cpf])
    row = cur.fetchone()
    if row:
        return (cpf, int(row[0]))
    nome = r.get('nome')
    if _valor_ausente(nome):
        nome = 'NOME NÃO INFORMADO'
    id_var = cur.var(int)
    cur.execute(
        '\n            INSERT INTO etl.pessoa (cpf, nome, data_nascimento, sexo, estado_civil, cor_raca, nacionalidade, email, telefone)\n            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)\n            RETURNING id INTO :10\n            ',
        [
            cpf,
            str(nome)[:512],
            _para_data(r.get('data_nascimento')),
            None if _valor_ausente(r.get('sexo')) else str(r['sexo'])[:20],
            None if _valor_ausente(r.get('estado_civil')) else str(r['estado_civil'])[:64],
            None if _valor_ausente(r.get('cor')) else str(r['cor'])[:32],
            None if _valor_ausente(r.get('nacionalidade')) else str(r['nacionalidade'])[:64],
            None if _valor_ausente(r.get('email')) else str(r['email'])[:256],
            None if _valor_ausente(r.get('telefone')) else str(r['telefone'])[:64],
            id_var,
        ],
    )
    pid = int(id_var.getvalue()[0])
    return (cpf, pid)

def _upsert_vinculo(cur: oracledb.Cursor, pid: int, r: dict, org_map: dict[str, int], car_map: dict[str, int], tv_map: dict[str, int], st_map: dict[str, int]) -> None:
    sigla = r.get('sigla_orgao')
    cargo_n = r.get('cargo')
    if _valor_ausente(sigla) or str(sigla) not in org_map:
        raise ValueError(f'Órgão inválido para CPF {r.get("cpf")}: {sigla!r}')
    if _valor_ausente(cargo_n) or str(cargo_n) not in car_map:
        raise ValueError(f'Cargo inválido para CPF {r.get("cpf")}: {cargo_n!r}')
    tv = r.get('tipo_vinculo')
    st = r.get('status_servidor')
    if _valor_ausente(tv) or str(tv) not in tv_map:
        raise ValueError(f'tipo_vinculo inválido para CPF {r.get("cpf")}: {tv!r}')
    if _valor_ausente(st) or str(st) not in st_map:
        raise ValueError(f'status_servidor inválido para CPF {r.get("cpf")}: {st!r}')
    cur.execute('DELETE FROM etl.vinculo WHERE pessoa_id = :1', [pid])
    cur.execute(
        '\n            INSERT INTO etl.vinculo (\n                pessoa_id, orgao_id, cargo_id, tipo_vinculo_id, status_servidor_id,\n                matricula, salario_base, lotacao, data_admissao, data_saida\n            ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)\n            ',
        (
            pid,
            org_map[str(sigla)],
            car_map[str(cargo_n)],
            tv_map[str(tv)],
            st_map[str(st)],
            None if _valor_ausente(r.get('matricula')) else str(r['matricula'])[:128],
            _para_float(r.get('salario_base')),
            None if _valor_ausente(r.get('lotacao')) else str(r['lotacao'])[:4000],
            _para_data(r.get('data_admissao')),
            _para_data(r.get('data_saida')),
        ),
    )
    nota = _para_float(r.get('avaliacao_desempenho'))
    ciclo = r.get('ciclo_avaliacao')
    if not _valor_ausente(ciclo) and nota is not None:
        cur.execute(
            '\n                MERGE INTO etl.avaliacao_desempenho d\n                USING (SELECT :1 AS pessoa_id, :2 AS ciclo, :3 AS nota FROM DUAL) s\n                ON (d.pessoa_id = s.pessoa_id AND d.ciclo_avaliacao = s.ciclo)\n                WHEN MATCHED THEN UPDATE SET d.nota = s.nota\n                WHEN NOT MATCHED THEN INSERT (pessoa_id, ciclo_avaliacao, nota)\n                VALUES (s.pessoa_id, s.ciclo, s.nota)\n                ',
            (pid, str(ciclo)[:16], nota),
        )

def _reconectar(max_tentativas: int, espera_segundos: int) -> tuple[oracledb.Connection, oracledb.Cursor]:
    ultima: Exception | None = None
    for tentativa in range(1, max_tentativas + 1):
        try:
            conn = conectar()
            return (conn, conn.cursor())
        except Exception as exc:  # pragma: no cover
            ultima = exc
            if tentativa < max_tentativas:
                print(f'Falha ao reconectar (tentativa {tentativa}/{max_tentativas}). Aguardando {espera_segundos}s...')
                time.sleep(espera_segundos)
    raise RuntimeError('Não foi possível reconectar ao Oracle após múltiplas tentativas.') from ultima

def _rollback_seguro(conn: oracledb.Connection) -> None:
    try:
        conn.rollback()
    except Exception:
        pass

def _construir_mapa_cpf_id(cur: oracledb.Cursor) -> dict[str, int]:
    cur.execute('SELECT id, cpf FROM etl.pessoa')
    return {str(r[1]): int(r[0]) for r in cur.fetchall() if r[1]}

def _carregar_pessoas_em_lotes(
    conn: oracledb.Connection,
    cur: oracledb.Cursor,
    df_con: pd.DataFrame,
    state: dict,
    batch_size: int,
    max_retries: int,
    retry_delay: int,
    org_map: dict[str, int],
    car_map: dict[str, int],
    tv_map: dict[str, int],
    st_map: dict[str, int],
) -> tuple[oracledb.Connection, oracledb.Cursor, dict[str, int]]:
    start_idx = int(state.get('last_person_idx', -1)) + 1
    cpf_to_id: dict[str, int] = {}
    total = len(df_con)
    for chunk_start in range(start_idx, total, batch_size):
        chunk_end = min(chunk_start + batch_size, total)
        tentativas = 0
        while True:
            try:
                for i in range(chunk_start, chunk_end):
                    r = df_con.iloc[i].to_dict()
                    cpf, pid = _upsert_pessoa(cur, r)
                    if not cpf or pid is None:
                        continue
                    cpf_to_id[cpf] = pid
                    _upsert_vinculo(cur, pid, r, org_map, car_map, tv_map, st_map)
                conn.commit()
                last_row = df_con.iloc[chunk_end - 1].to_dict()
                state.update(
                    {
                        'phase': PERSON_PHASE,
                        'last_person_idx': chunk_end - 1,
                        'last_person_cpf': _normalizar_cpf(last_row.get('cpf')),
                    }
                )
                _salvar_estado_carga(state)
                print(f'Lote pessoas {chunk_start}-{chunk_end - 1} confirmado.')
                break
            except Exception as exc:
                _rollback_seguro(conn)
                tentativas += 1
                if tentativas > max_retries:
                    raise
                print(
                    f'Erro em lote de pessoas {chunk_start}-{chunk_end - 1}: {exc}. '
                    f'Retentando com reconexão ({tentativas}/{max_retries})...'
                )
                try:
                    cur.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
                conn, cur = _reconectar(max_retries, retry_delay)
                tv_map, st_map, _ = _carregar_mapas_catalogo(cur)
                org_map, car_map = _garantir_orgaos_cargos(cur, df_con)
                time.sleep(retry_delay)
    return (conn, cur, cpf_to_id)

def _carregar_eventos_em_lotes(
    conn: oracledb.Connection,
    cur: oracledb.Cursor,
    df_es: pd.DataFrame,
    state: dict,
    batch_size: int,
    max_retries: int,
    retry_delay: int,
    cpf_to_id: dict[str, int],
    te_map: dict[str, int],
) -> tuple[oracledb.Connection, oracledb.Cursor, int]:
    start_idx = int(state.get('last_event_idx', -1)) + 1
    inserted_total = 0
    total = len(df_es)
    for chunk_start in range(start_idx, total, batch_size):
        chunk_end = min(chunk_start + batch_size, total)
        tentativas = 0
        while True:
            try:
                inserted = carregar_eventos_esocial(
                    cur,
                    df_es.iloc[chunk_start:chunk_end],
                    cpf_to_id,
                    te_map,
                )
                conn.commit()
                inserted_total += inserted
                last_row = df_es.iloc[chunk_end - 1].to_dict()
                state.update(
                    {
                        'phase': EVENT_PHASE,
                        'last_event_idx': chunk_end - 1,
                        'last_event_id': None if _valor_ausente(last_row.get('id_evento')) else str(last_row.get('id_evento')),
                    }
                )
                _salvar_estado_carga(state)
                print(f'Lote eventos {chunk_start}-{chunk_end - 1} confirmado.')
                break
            except Exception as exc:
                _rollback_seguro(conn)
                tentativas += 1
                if tentativas > max_retries:
                    raise
                print(
                    f'Erro em lote de eventos {chunk_start}-{chunk_end - 1}: {exc}. '
                    f'Retentando com reconexão ({tentativas}/{max_retries})...'
                )
                try:
                    cur.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
                conn, cur = _reconectar(max_retries, retry_delay)
                _, _, te_map = _carregar_mapas_catalogo(cur)
                time.sleep(retry_delay)
    return (conn, cur, inserted_total)

def carregar_eventos_esocial(cur: oracledb.Cursor, df: pd.DataFrame, cpf_to_pid: dict[str, int], te_map: dict[str, int]) -> int:
    inserted = 0
    for _, row in df.iterrows():
        r = row.to_dict()
        cpf = _normalizar_cpf(r.get('cpf'))
        if not cpf or cpf not in cpf_to_pid:
            continue
        pid = cpf_to_pid[cpf]
        te = r.get('tipo_evento')
        if _valor_ausente(te) or str(te) not in te_map:
            continue
        id_ev = r.get('id_evento')
        de = r.get('data_evento')
        if _valor_ausente(id_ev) or _valor_ausente(de):
            continue
        id_ev_s = str(id_ev)[:128]
        cur.execute('\n            INSERT INTO etl.evento (\n                pessoa_id, id_evento_origem, tipo_evento_id, data_evento,\n                codigo_categoria, indicador_retificacao, numero_recibo\n            )\n            SELECT :1, :2, :3, :4, :5, :6, :7 FROM DUAL\n            WHERE NOT EXISTS (SELECT 1 FROM etl.evento e WHERE e.id_evento_origem = :8)\n            ', (pid, id_ev_s, te_map[str(te)], _para_data(de), None if _valor_ausente(r.get('codigo_categoria')) else str(r['codigo_categoria'])[:32], None if _valor_ausente(r.get('indicador_retificacao')) else str(r['indicador_retificacao'])[:32], None if _valor_ausente(r.get('numero_recibo')) else str(r['numero_recibo'])[:128], id_ev_s))
        if cur.rowcount and cur.rowcount > 0:
            inserted += 1
    return inserted

def principal() -> None:
    parser = argparse.ArgumentParser(description='Carga Oracle XE (3FN)')
    parser.add_argument('--sample', type=int, default=None, help='Limita linhas do consolidado (debug)')
    parser.add_argument('--no-truncate', action='store_true', help='Não esvazia tabelas fato/catálogo de carga')
    parser.add_argument('--batch-size', type=int, default=500, help='Tamanho do lote para commit incremental')
    parser.add_argument('--max-retries', type=int, default=3, help='Quantidade de retentativas por lote em falha de rede')
    parser.add_argument('--retry-delay', type=int, default=3, help='Espera (segundos) entre retentativas/reconexão')
    parser.add_argument('--clear-load-checkpoint', action='store_true', help='Limpa checkpoint da carga Oracle antes de iniciar')
    args = parser.parse_args()
    con_path = PROCESSED_DIR / 'pessoas_consolidado.csv'
    es_path = PROCESSED_DIR / 'esocial.csv'
    if not con_path.is_file():
        raise SystemExit(f'Arquivo não encontrado: {con_path}. Rode antes: python main.py')
    df_con = pd.read_csv(con_path, encoding='utf-8-sig', dtype={'cpf': 'string'})
    if args.sample:
        df_con = df_con.head(args.sample)
    if args.clear_load_checkpoint:
        _limpar_estado_carga()
    state = _carregar_estado_carga()
    _validar_estado_carga(state, args.sample, args.no_truncate)
    state.update({'sample': args.sample, 'no_truncate': bool(args.no_truncate)})
    print(f'Conectando: {obter_url_do_banco()!r}')
    conn, cur = _reconectar(max_tentativas=args.max_retries, espera_segundos=args.retry_delay)
    try:
        tv_map, st_map, te_map = _carregar_mapas_catalogo(cur)
        should_truncate = (not args.no_truncate) and (not state.get('phase'))
        if should_truncate:
            _esvaziar_tabelas_carga(cur)
            conn.commit()
            cur = conn.cursor()
            tv_map, st_map, te_map = _carregar_mapas_catalogo(cur)
        elif not args.no_truncate:
            print('Retomada detectada: mantendo dados já carregados (sem novo TRUNCATE).')
        org_map, car_map = _garantir_orgaos_cargos(cur, df_con)
        conn, cur, cpf_to_id_incremental = _carregar_pessoas_em_lotes(
            conn,
            cur,
            df_con,
            state,
            args.batch_size,
            args.max_retries,
            args.retry_delay,
            org_map,
            car_map,
            tv_map,
            st_map,
        )
        cur = conn.cursor()
        te_map = _carregar_mapas_catalogo(cur)[2]
        cpf_to_id = _construir_mapa_cpf_id(cur)
        cpf_to_id.update(cpf_to_id_incremental)
        n_ev = 0
        if es_path.is_file():
            df_es = pd.read_csv(es_path, encoding='utf-8-sig', dtype={'cpf': 'string'})
            if args.sample:
                cpfs = {_normalizar_cpf(x) for x in df_con['cpf']}
                cpfs.discard(None)
                df_es = df_es[df_es['cpf'].map(lambda x: _normalizar_cpf(x)).isin(cpfs)]
            conn, cur, n_ev = _carregar_eventos_em_lotes(
                conn,
                cur,
                df_es.reset_index(drop=True),
                state,
                args.batch_size,
                args.max_retries,
                args.retry_delay,
                cpf_to_id,
                te_map,
            )
        _limpar_estado_carga()
        print(f'Pessoas/vínculos carregados: {len(cpf_to_id)}')
        print(f'Eventos inseridos (linhas novas): {n_ev}')
        print('Carga concluída com sucesso.')
    except Exception:
        _rollback_seguro(conn)
        raise
    finally:
        cur.close()
        conn.close()
if __name__ == '__main__':
    principal()

from __future__ import annotations
import argparse
import sys
from datetime import date, datetime
from pathlib import Path
import oracledb
import pandas as pd
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from src.config import PROCESSED_DIR
from src.db import conectar, obter_url_do_banco

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
    args = parser.parse_args()
    con_path = PROCESSED_DIR / 'pessoas_consolidado.csv'
    es_path = PROCESSED_DIR / 'esocial.csv'
    if not con_path.is_file():
        raise SystemExit(f'Arquivo não encontrado: {con_path}. Rode antes: python main.py')
    df_con = pd.read_csv(con_path, encoding='utf-8-sig', dtype={'cpf': 'string'})
    if args.sample:
        df_con = df_con.head(args.sample)
    print(f'Conectando: {obter_url_do_banco()!r}')
    conn = conectar()
    cur = conn.cursor()
    try:
        tv_map, st_map, te_map = _carregar_mapas_catalogo(cur)
        if not args.no_truncate:
            _esvaziar_tabelas_carga(cur)
            conn.commit()
            cur = conn.cursor()
            tv_map, st_map, te_map = _carregar_mapas_catalogo(cur)
        org_map, car_map = _garantir_orgaos_cargos(cur, df_con)
        cpf_to_id = carregar_consolidado(cur, df_con, org_map, car_map, tv_map, st_map)
        conn.commit()
        cur = conn.cursor()
        cur.execute('SELECT id, codigo FROM etl.tipo_evento')
        te_map = {r[1]: r[0] for r in cur.fetchall()}
        n_ev = 0
        if es_path.is_file():
            df_es = pd.read_csv(es_path, encoding='utf-8-sig', dtype={'cpf': 'string'})
            if args.sample:
                cpfs = {_normalizar_cpf(x) for x in df_con['cpf']}
                cpfs.discard(None)
                df_es = df_es[df_es['cpf'].map(lambda x: _normalizar_cpf(x)).isin(cpfs)]
            n_ev = carregar_eventos_esocial(cur, df_es, cpf_to_id, te_map)
        conn.commit()
        print(f'Pessoas/vínculos carregados: {len(cpf_to_id)}')
        print(f'Eventos inseridos (linhas novas): {n_ev}')
        print('Carga concluída com sucesso.')
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
if __name__ == '__main__':
    principal()

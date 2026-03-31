from __future__ import annotations
import csv
import importlib.util
import random
from pathlib import Path
_SCRIPTS = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location('_gs', _SCRIPTS / 'generate_servidores.py')
gs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gs)
N = 200

def orgaos_contagem_escalada(n: int) -> list[tuple[str, int]]:
    total_full = sum((c for _, c in gs.ORG_COUNTS))
    if n <= 0:
        raise ValueError('n deve ser positivo')
    if n >= total_full:
        return list(gs.ORG_COUNTS)
    exact = [c * n / total_full for _, c in gs.ORG_COUNTS]
    floors = [int(x) for x in exact]
    rem = n - sum(floors)
    order = sorted(range(len(gs.ORG_COUNTS)), key=lambda i: exact[i] - floors[i], reverse=True)
    for k in range(rem):
        floors[order[k]] += 1
    out = [(gs.ORG_COUNTS[i][0], floors[i]) for i in range(len(gs.ORG_COUNTS)) if floors[i] > 0]
    assert sum((q for _, q in out)) == n
    return out

def principal() -> None:
    random.seed(42)
    counts = orgaos_contagem_escalada(N)
    old = gs.ORG_COUNTS
    gs.ORG_COUNTS = counts
    try:
        personas = gs.montar_personas(N)
    finally:
        gs.ORG_COUNTS = old
    gs.DATA_DIR.mkdir(parents=True, exist_ok=True)
    rh_path = gs.DATA_DIR / 'rh.csv'
    esocial_path = gs.DATA_DIR / 'esocial.csv'
    gestao_path = gs.DATA_DIR / 'gestao.csv'
    with rh_path.open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(['nome', 'cpf', 'data_nascimento', 'sigla_orgao', 'data_admissao', 'data_saida', 'matricula', 'cargo', 'tipo_vinculo', 'salario_base', 'lotacao', 'status_servidor'])
        for p in personas:
            w.writerow([p['nome'], p['cpf_rh'], gs.formatar_iso(p['data_nascimento']), p['sigla_orgao'], gs.formatar_iso(p['data_admissao']), gs.formatar_iso(p['data_saida']) if p['data_saida'] else '', p['matricula_rh'], p['cargo'], p['tipo_vinculo'], p['salario_base'], p['lotacao'], p['status_servidor']])
    with esocial_path.open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(['nome', 'cpf', 'data_nascimento', 'sigla_orgao', 'id_evento', 'tipo_evento', 'data_evento', 'codigo_categoria', 'indicador_retificacao', 'numero_recibo'])
        for p in personas:
            w.writerow([p['nome'], p['cpf'], gs.formatar_iso(p['data_nascimento']), p['sigla_orgao'], p['id_evento'], 'admissao', gs.formatar_iso(p['data_admissao']), '301', 'original', f"REC-{p['data_admissao'].year}-{gs.chave_matricula(p['sigla_orgao'])}-{p['cpf'][:6]}"])
    with gestao_path.open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(['nome', 'cpf', 'dataNascimento', 'orgao', 'dataEntrada', 'dataSaida', 'matricula', 'cargo', 'tipoVinculo', 'salario', 'unidadeLotacao', 'status', 'sexo', 'estadoCivil', 'cor', 'nacionalidade', 'email', 'telefone', 'avaliacaoDesempenho', 'cicloAvaliacao'])
        for p in personas:
            w.writerow([p['nome'], p['cpf'], gs.formatar_br_data(p['data_nascimento']), p['sigla_orgao'], gs.formatar_br_data(p['data_admissao']), gs.formatar_br_data(p['data_saida']) if p['data_saida'] else '', p['matricula_ges'], p['cargo'], p['tipo_vinculo'], p['salario_base'], p['lotacao'], p['status_servidor'], p['sexo'], p['estado_civil'], p['cor'], 'Brasileira', p['email'], p['telefone'], p['avaliacao'], p['ciclo_aval']])
    print(f'Gerados {N} servidores (amostra) em:')
    print(rh_path)
    print(esocial_path)
    print(gestao_path)
if __name__ == '__main__':
    principal()

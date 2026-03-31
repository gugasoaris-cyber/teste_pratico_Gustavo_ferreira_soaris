from __future__ import annotations
import csv
import random
import secrets
from datetime import date, timedelta
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / 'data'
ORG_COUNTS: list[tuple[str, int]] = [('CGE', 199), ('PGE', 367), ('SEAD', 2692), ('ECONOMIA', 3108), ('DETRAN', 1274), ('SSP', 1586), ('CBM', 2675), ('DGPP', 3316), ('DGPC', 4738), ('SES', 7589), ('PM', 11647), ('SEDF', 38), ('FAPEG', 78), ('SECOM', 110), ('VICEGOV', 111), ('GOIAS TURISMO', 111), ('SERINT', 129), ('SEINFRA', 130), ('CASA CIVIL', 138), ('SEAPA', 152), ('SECTI', 178), ('JUCEG', 188), ('AGR', 192), ('SIC', 206), ('RETOMADA', 253), ('SECULT', 259), ('SEEL', 385), ('ABC', 390), ('SECAMI', 460), ('SEMAD', 522), ('SGG', 577), ('GOINFRA', 737), ('EMATER', 828), ('DPE-GO', 845), ('SEDS', 966), ('AGRODEFESA', 1004), ('UEG', 2001), ('SEDUC', 44981), ('GOIASPREV', 71101)]
NOMES = 'Ana Bruno Carla Daniel Eduarda Fábio Gabriela Hugo Isabela João Karina Lucas Mariana Nicolas Olívia Paulo Quitéria Rafael Sofia Thiago André Beatriz Camila Diego Elisa Felipe Gustavo Helena Igor Juliana Leonardo Marina Natália Otávio Patrícia Renata Samuel Tatiana Vinícius Yasmin'.split()
SOBRENOMES = 'Silva Santos Oliveira Souza Rodrigues Ferreira Alves Pereira Lima Gomes Ribeiro Carvalho Almeida Martins Rocha Dias Barbosa Cardoso Nogueira Moura Teixeira Freitas Barros Araújo Melo Monteiro Duarte Lopes Nunes Vieira Costa Ramos Correia Castro Fernandes Machado Pinto Cavalcanti'.split()
CARGOS = 'Analista Administrativo Assistente Técnico Contador Engenheiro Médico Enfermeiro Professor Motorista Advogado Auditor Tecnologista Assessor Pedagogo Psicólogo Contador Analista TI Segurança Administrador'.split()
TIPO_VINCULO = ('efetivo', 'comissionado', 'cedido')
STATUS_RH = ('ativo', 'afastado', 'exonerado')
STATUS_WEIGHTS = (0.88, 0.09, 0.03)
SEXO = ('M', 'F')
ESTADO_CIVIL = ('solteiro', 'solteira', 'casado', 'casada', 'divorciado', 'divorciada', 'viúvo', 'viúva', 'união estável')
COR = ('parda', 'branca', 'negra', 'amarela')

def gerar_cpf() -> str:
    n = [secrets.randbelow(10) for _ in range(9)]
    if len(set(n)) == 1:
        n[8] = (n[8] + 1) % 10
    s1 = sum(((10 - i) * n[i] for i in range(9))) % 11
    d1 = 0 if s1 < 2 else 11 - s1
    n.append(d1)
    s2 = sum(((11 - i) * n[i] for i in range(10))) % 11
    d2 = 0 if s2 < 2 else 11 - s2
    n.append(d2)
    return ''.join((str(x) for x in n))

def cpfs_unicos(n: int) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    while len(out) < n:
        c = gerar_cpf()
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out

def chave_matricula(sigla: str) -> str:
    return sigla.replace(' ', '').replace('-', '')

def nascimento_aleatorio() -> date:
    start = date(1960, 1, 1)
    end = date(2005, 12, 31)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def adicionar_anos_calendario(d: date, years: int) -> date:
    y = d.year + years
    try:
        return date(y, d.month, d.day)
    except ValueError:
        return date(y, d.month, 28)

def admissao_apos_nascimento(birth: date) -> date:
    min_age = adicionar_anos_calendario(birth, 18)
    if min_age < date(1995, 1, 1):
        min_age = date(1995, 1, 1)
    end = date(2025, 12, 31)
    if min_age > end:
        min_age = birth + timedelta(days=365 * 18)
    span = (end - min_age).days
    return min_age + timedelta(days=random.randint(0, max(span, 1)))

def formatar_iso(d: date) -> str:
    return d.isoformat()

def formatar_br_data(d: date) -> str:
    return d.strftime('%d/%m/%Y')

def formatar_cpf_rh(cpf: str) -> str:
    return f'{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}'

def sortear_status() -> str:
    return random.choices(STATUS_RH, weights=STATUS_WEIGHTS, k=1)[0]

def nome_completo_aleatorio() -> str:
    return f'{random.choice(NOMES)} {random.choice(SOBRENOMES)} {random.choice(SOBRENOMES)}'

def montar_personas(total: int) -> list[dict]:
    cpfs = cpfs_unicos(total)
    personas: list[dict] = []
    idx = 0
    event_seq = 0
    for sigla, qtd in ORG_COUNTS:
        slug = chave_matricula(sigla)
        for seq in range(1, qtd + 1):
            cpf = cpfs[idx]
            birth = nascimento_aleatorio()
            adm = admissao_apos_nascimento(birth)
            status = sortear_status()
            if status == 'exonerado':
                saida = adm + timedelta(days=random.randint(180, 4000))
                if saida > date.today():
                    saida = date.today() - timedelta(days=30)
            else:
                saida = None
            nome = nome_completo_aleatorio()
            mat_rh = f'RH-{slug}-{seq:06d}'
            mat_ges = f'GES-{slug}-{seq:06d}'
            event_seq += 1
            personas.append({'nome': nome, 'cpf': cpf, 'cpf_rh': formatar_cpf_rh(cpf), 'data_nascimento': birth, 'sigla_orgao': sigla, 'data_admissao': adm, 'data_saida': saida, 'matricula_rh': mat_rh, 'matricula_ges': mat_ges, 'cargo': random.choice(CARGOS), 'tipo_vinculo': random.choices(TIPO_VINCULO, weights=(0.82, 0.14, 0.04), k=1)[0], 'salario_base': round(random.uniform(2800, 14500), 2), 'lotacao': f'Unidade {slug} - Setor {random.randint(1, 40)}', 'status_servidor': status, 'id_evento': f'ESOC-{slug}-{event_seq:08d}', 'sexo': random.choice(SEXO), 'estado_civil': random.choice(ESTADO_CIVIL), 'cor': random.choice(COR), 'email': f'servidor.{cpf}@goias.gov.br', 'telefone': f'(62) 9{random.randint(1000, 9999)}-{random.randint(1000, 9999)}', 'avaliacao': round(random.uniform(3.0, 5.0), 1), 'ciclo_aval': random.choice(('2023', '2024', '2025'))})
            idx += 1
    return personas

def principal() -> None:
    random.seed(42)
    total = sum((c for _, c in ORG_COUNTS))
    assert total == 166261, f'Total esperado 166261, obtido {total}'
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    personas = montar_personas(total)
    rh_path = DATA_DIR / 'rh.csv'
    esocial_path = DATA_DIR / 'esocial.csv'
    gestao_path = DATA_DIR / 'gestao.csv'
    with rh_path.open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(['nome', 'cpf', 'data_nascimento', 'sigla_orgao', 'data_admissao', 'data_saida', 'matricula', 'cargo', 'tipo_vinculo', 'salario_base', 'lotacao', 'status_servidor'])
        for p in personas:
            w.writerow([p['nome'], p['cpf_rh'], formatar_iso(p['data_nascimento']), p['sigla_orgao'], formatar_iso(p['data_admissao']), formatar_iso(p['data_saida']) if p['data_saida'] else '', p['matricula_rh'], p['cargo'], p['tipo_vinculo'], p['salario_base'], p['lotacao'], p['status_servidor']])
    with esocial_path.open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(['nome', 'cpf', 'data_nascimento', 'sigla_orgao', 'id_evento', 'tipo_evento', 'data_evento', 'codigo_categoria', 'indicador_retificacao', 'numero_recibo'])
        for p in personas:
            w.writerow([p['nome'], p['cpf'], formatar_iso(p['data_nascimento']), p['sigla_orgao'], p['id_evento'], 'admissao', formatar_iso(p['data_admissao']), '301', 'original', f"REC-{p['data_admissao'].year}-{chave_matricula(p['sigla_orgao'])}-{p['cpf'][:6]}"])
    with gestao_path.open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.writer(f)
        w.writerow(['nome', 'cpf', 'dataNascimento', 'orgao', 'dataEntrada', 'dataSaida', 'matricula', 'cargo', 'tipoVinculo', 'salario', 'unidadeLotacao', 'status', 'sexo', 'estadoCivil', 'cor', 'nacionalidade', 'email', 'telefone', 'avaliacaoDesempenho', 'cicloAvaliacao'])
        for p in personas:
            w.writerow([p['nome'], p['cpf'], formatar_br_data(p['data_nascimento']), p['sigla_orgao'], formatar_br_data(p['data_admissao']), formatar_br_data(p['data_saida']) if p['data_saida'] else '', p['matricula_ges'], p['cargo'], p['tipo_vinculo'], p['salario_base'], p['lotacao'], p['status_servidor'], p['sexo'], p['estado_civil'], p['cor'], 'Brasileira', p['email'], p['telefone'], p['avaliacao'], p['ciclo_aval']])
    print(f'Gerados {total} servidores em:')
    print(rh_path)
    print(esocial_path)
    print(gestao_path)
if __name__ == '__main__':
    principal()

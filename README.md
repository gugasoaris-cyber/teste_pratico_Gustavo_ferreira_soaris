# ETL de Pessoas com Oracle XE

Projeto de ETL para consolidação de dados de pessoas a partir de três fontes (`RH`, `eSocial`, `Gestão`), com padronização, validação, deduplicação, geração de relatórios e carga em banco relacional Oracle.

## Objetivo

Atender ao cenário do teste prático com:
- pipeline ETL reprodutível
- dados normalizados em modelo relacional
- logs e evidências de qualidade de dados
- carga final no Oracle XE via Docker

## Tecnologias

- Python 3
- pandas
- oracledb
- Docker + Docker Compose
- Oracle Database XE 21 (`gvenzl/oracle-xe`)

## Estrutura principal

- `main.py`: execução da ETL (extração, transformação, merge e saídas)
- `scripts/load_oracle.py`: carga dos dados processados no Oracle
- `executar_etl_completo.py`: automação de ponta a ponta (Docker + ETL + carga)
- `docker-compose.yml`: Oracle XE local
- `db/init/01_schema.sql`: schema Oracle
- `data/`: entradas CSV
- `data/processed/`: saídas processadas
- `logs/`: métricas e relatórios de auditoria

## Pré-requisitos

1. Python 3 instalado
2. Docker Desktop instalado

Validação rápida:

```bash
python --version
docker --version
docker compose version
```

## Instalação

Na raiz do projeto:

```bash
pip install -r requirements.txt
```

Opcional:
- copiar `.env.example` para `.env`
- ajustar `DATABASE_URL` se necessário

## Execução rápida (recomendada)

Pré-condição: Docker Desktop em execução.

O comando abaixo já sobe o Oracle com `docker compose up -d`, aguarda o banco ficar saudável e executa ETL + carga:

```bash
python executar_etl_completo.py
```

Exemplos úteis:

```bash
# Gera CSVs de amostra (200 linhas) antes de rodar ETL e carga
python executar_etl_completo.py --gerar-dados 200

# Gera os CSVs completos (cenário Anexo I) antes de rodar ETL e carga
python executar_etl_completo.py --gerar-dados completo

# Limita a N linhas por fonte na ETL e alinha a carga ao mesmo recorte
python executar_etl_completo.py --sample 1000

# Carrega no Oracle sem truncar tabelas antes (incremental; evita apagar dados já carregados)
python executar_etl_completo.py --no-truncate

# Pula subir Docker/aguardar Oracle (use se o container já estiver rodando e saudável)
python executar_etl_completo.py --skip-docker
```

## Execução manual (passo a passo)

### 1) Subir Oracle

```bash
docker compose up -d
```

### 2) Gerar dados de entrada (opcional)

```bash
python scripts/generate_servidores.py
```

ou

```bash
python scripts/generate_servidores_200.py
```

### 3) Rodar ETL

```bash
python main.py
```

### 4) Carregar no Oracle

```bash
python scripts/load_oracle.py
```

## Conexão no Oracle (DBeaver/Power BI)

- Host: `localhost` (ou `127.0.0.1`)
- Porta: `1521`
- Service Name: `XEPDB1`
- Usuário: `etl`
- Senha: `etl_secret`

No Power BI, pode ser necessário instalar o OCMT (Oracle Client for Microsoft Tools).

## Saídas geradas

- `data/processed/pessoas_consolidado.csv`
- `data/processed/rh.csv`
- `data/processed/esocial.csv`
- `data/processed/gestao.csv`
- `logs/etl_metrics.json`
- `logs/duplicidade_cpf.txt`
- `logs/rejeitados.csv` (quando houver)
- `logs/duplicados.csv` (quando houver)

## Critérios atendidos (resumo)

- ETL de múltiplas fontes para banco relacional
- padronização e validação de dados
- detecção de rejeições e duplicidades com relatório
- deduplicação e merge por prioridade (`RH > eSocial > Gestão`)
- retomada após falha via checkpoint
- métricas de execução
- carga Oracle em modelo normalizado

## Documentação e painel (entrega)

- Documento Word da entrega (requisitos, casos de uso, diagrama de fluxo...): `documentacao/documentacao.docx`
- Fluxo visual do sistema: `documentacao/fluxo_sistema.png`
- Painel Power BI: `painel_powerBI/BI_servidores_VOBS.pbix`



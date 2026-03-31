# ETL de Pessoas com Oracle XE

Projeto de ETL para consolidação de dados de pessoas a partir de três fontes (`RH`, `eSocial`, `Gestão`), com padronização, validação, deduplicação, geração de relatórios e carga em banco relacional Oracle.

## Objetivo

Atender ao cenário do teste prático com:
- pipeline ETL reprodutível
- dados normalizados em modelo relacional
- logs e evidências de qualidade de dados
- carga final no Oracle XE via Docker

Contexto de origem dos dados no projeto:
- neste repositório, as fontes são arquivos `CSV` em `data/` (`rh.csv`, `esocial.csv`, `gestao.csv`);
- em cenários reais, o mesmo fluxo pode receber dados de `CSV`, `API` ou tabelas de outros bancos.

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

1. Python 3.10, 3.11 ou 3.12 instalado (recomendado: 3.12)
2. Docker Desktop instalado

Validação rápida:

```bash
python --version
docker --version
docker compose version
```

## Instalação

Na raiz do projeto:

> Se você estiver usando Python 3.13+ (ex.: 3.14), a instalação pode falhar no pacote `oracledb` no Windows por falta de wheel compatível.
> Nesse caso, use Python 3.12 (recomendado) para evitar necessidade de compilação nativa.
>
> A virtualização (`venv`) é importante neste projeto porque a dependência `oracledb` (conexão com Oracle) pode falhar em versões mais novas do Python no Windows. Com ambiente virtual em Python 3.12, você fixa uma versão compatível para o Oracle e evita conflito com o Python global da máquina segue abaixo o codigo para fazer a virtualização.
```bash
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python --version
python -m pip --version
python -m pip install -r requirements.txt
```

Opcional:
- copiar `.env.example` para `.env`
- ajustar `DATABASE_URL` se necessário

## Execução rápida (validação rapida do sistema)

Pré-condição: Docker Desktop em execução.

O comando abaixo já sobe o Oracle com `docker compose up -d`, aguarda o banco ficar saudável e executa ETL + carga:

```bash
python executar_etl_completo.py
```

Exemplos úteis:

```bash
# Gera CSVs de amostra (200 linhas) antes de rodar ETL e carga
python executar_etl_completo.py

# Gera os CSVs completos (cenário Anexo I) antes de rodar ETL e carga
python executar_etl_completo.py --gerar-dados completo

# Limita a N linhas por fonte na ETL e alinha a carga ao mesmo recorte
python executar_etl_completo.py --sample 1000

# Carrega no Oracle sem truncar tabelas antes (incremental; evita apagar dados já carregados)
python executar_etl_completo.py --no-truncate

# Carga Oracle em lotes com retentativa/reconexão em falhas transitórias
python scripts/load_oracle.py --batch-size 500 --max-retries 3 --retry-delay 3

# Reinicia a carga do zero (ignora checkpoint da carga Oracle)
python scripts/load_oracle.py --clear-load-checkpoint

# Pula subir Docker/aguardar Oracle (use se o container já estiver rodando e saudável)
python executar_etl_completo.py --skip-docker
```

Observação: a carga Oracle usa, por padrão, **lotes de 500 em 500 registros** (`--batch-size 500`).

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

# Limita a N linhas por fonte na ETL (exemplo abaixo está 2500 mas você pode colocar outros valores)
python main.py --sample 2500
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

### Erro comum no Power BI e como resolver

Se ao atualizar no Power BI aparecer o erro:

`DataSource.Error: Nenhum driver ODAC foi encontrado no sistema`

faça os passos abaixo:

1. Feche o Power BI Desktop.
2. Instale o **Oracle Client for Microsoft Tools (OCMT) 64-bit**:
   - https://go.microsoft.com/fwlink/p/?LinkID=272376
3. Reabra o Power BI (se necessário, reinicie o Windows).
4. Tente a atualização novamente.

Observações importantes:
- O Power BI Desktop e o driver Oracle devem ter a mesma arquitetura (64-bit).
- Se o Oracle estiver em Docker, confirme que o container está rodando (`docker compose up -d`).
- Em alguns cenários, instalar o ODAC/OCMT resolve esse erro imediatamente sem alterar o ETL.

## Saídas geradas

- `data/processed/pessoas_consolidado.csv`
- `data/processed/rh.csv`
- `data/processed/esocial.csv`
- `data/processed/gestao.csv`
- `logs/etl_metrics.json`
- `logs/duplicidade_cpf.txt`
- `logs/rejeitados.csv` (quando houver)
- `logs/duplicados.csv` (quando houver)

## Como os requisitos foram atendidos

- **Normalização de dados:** padronização de tipos e formatos (CPF, datas, campos textuais) antes do merge e da carga.
- **Integridade de dados:** validações de obrigatoriedade/domínio e uso de modelo relacional normalizado no Oracle (`pessoa`, `vinculo`, `evento`, catálogos).
- **Detecção de erro com relatório:** geração de evidências em `logs/rejeitados.csv`, `logs/duplicados.csv` e `logs/duplicidade_cpf.txt`.
- **Padronização de formatos:** saídas tratadas em `data/processed/` com estrutura consistente para consumo da carga Oracle.
- **Robustez contra erros na execução:** retomada por checkpoint em duas camadas:
  - ETL: `main.py` + `src/pipeline_resume.py` (estado em `logs/etl_pipeline_state.json` e artefatos em `data/processed/_checkpoint/`);
  - carga Oracle: `scripts/load_oracle.py` (estado em `logs/oracle_load_state.json`, commit por lote, retry e reconexão).
- **Métricas de execução:** consolidadas em `logs/etl_metrics.json`.

## Critérios atendidos (resumo)

- ETL de múltiplas fontes para banco relacional
- padronização e validação de dados
- detecção de rejeições e duplicidades com relatório
- deduplicação e merge por prioridade (`RH > eSocial > Gestão`)
- retomada após falha via checkpoint (ETL e carga Oracle)
- métricas de execução
- carga Oracle em modelo normalizado

## Documentação e painel (extra)

- Documento Word da entrega (requisitos, casos de uso, diagrama de fluxo...): `documentacao/documentacao.docx`
- Fluxo visual do sistema: `documentacao/fluxo_sistema.png`
- Painel Power BI: `painel_powerBI/BI_servidores_VOBS.pbix` (se ocorrer erro de driver Oracle/ODAC, ver seção `Erro comum no Power BI e como resolver`)



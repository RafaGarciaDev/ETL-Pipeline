# 🚀 ETL Pipeline End-to-End

[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/airflow-2.8+-green.svg)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-15+-blue.svg)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Pipeline ETL (Extract, Transform, Load) completo e escalável para processar dados de múltiplas fontes, aplicar transformações complexas e carregar em data warehouse. Projeto desenvolvido seguindo as melhores práticas da engenharia de dados.

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Funcionalidades](#funcionalidades)
- [Tecnologias](#tecnologias)
- [Início Rápido](#início-rápido)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Casos de Uso](#casos-de-uso)

## 🎯 Visão Geral

Este projeto demonstra um pipeline ETL completo que:

- **Extrai** dados de múltiplas fontes (APIs, CSV, Bancos de Dados)
- **Transforma** dados aplicando limpeza, validação e enriquecimento
- **Carrega** dados em PostgreSQL com estratégias incrementais e full-load
- **Orquestra** processos usando Apache Airflow
- **Monitora** execução com logs detalhados e alertas
- **Testa** código com cobertura automatizada

### Fontes de Dados

1. **API REST** - Dados de clima (OpenWeatherMap)
2. **CSV Files** - Dados de vendas e transações
3. **PostgreSQL** - Dados operacionais
4. **Web Scraping** - Dados públicos (opcional)

### Destino

- **PostgreSQL** - Data Warehouse dimensional

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                    FONTES DE DADOS                           │
├─────────────┬──────────────┬──────────────┬─────────────────┤
│   API REST  │  CSV Files   │  PostgreSQL  │  Web Scraping   │
└──────┬──────┴──────┬───────┴──────┬───────┴────────┬────────┘
       │             │              │                │
       ▼             ▼              ▼                ▼
┌─────────────────────────────────────────────────────────────┐
│                      EXTRACT LAYER                           │
│  • API Connector   • File Reader   • DB Connector            │
│  • Error Handling  • Retry Logic   • Data Validation         │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    RAW DATA STORAGE                          │
│             (data/raw - Staging Area)                        │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    TRANSFORM LAYER                           │
│  • Data Cleaning    • Deduplication   • Type Conversion      │
│  • Validation       • Enrichment      • Business Rules       │
│  • Aggregation      • Feature Engineering                    │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                 PROCESSED DATA STORAGE                       │
│            (data/processed - Clean Data)                     │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      LOAD LAYER                              │
│  • Incremental Load  • Full Refresh  • Upsert Strategy       │
│  • Data Quality Check • Transaction Management               │
└──────────────────────────┬──────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   DATA WAREHOUSE                             │
│              PostgreSQL - Star Schema                        │
│    • Fact Tables    • Dimension Tables    • Indexes          │
└─────────────────────────────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                 ORCHESTRATION & MONITORING                   │
│     Apache Airflow + Logging + Alertas                       │
└─────────────────────────────────────────────────────────────┘
```

## ✨ Funcionalidades

### Extract (Extração)

- ✅ **API REST Integration** - Extração de APIs com paginação
- ✅ **File Processing** - Suporte para CSV, JSON, Parquet
- ✅ **Database Extraction** - Queries incrementais
- ✅ **Error Handling** - Retry logic e fallback
- ✅ **Rate Limiting** - Controle de requisições

### Transform (Transformação)

- ✅ **Data Cleaning** - Remoção de duplicatas, valores nulos
- ✅ **Type Conversion** - Conversão de tipos de dados
- ✅ **Validation** - Great Expectations para qualidade
- ✅ **Enrichment** - Cálculos derivados e joins
- ✅ **Aggregation** - Métricas e KPIs

### Load (Carga)

- ✅ **Incremental Load** - Apenas dados novos/modificados
- ✅ **Full Refresh** - Recarga completa quando necessário
- ✅ **Upsert Strategy** - Insert ou Update conforme necessário
- ✅ **Transaction Management** - ACID compliance
- ✅ **Performance Optimization** - Batch processing

### Orquestração

- ✅ **Apache Airflow** - Agendamento e execução
- ✅ **DAG Dependencies** - Gerenciamento de dependências
- ✅ **Retry Logic** - Recuperação automática de falhas
- ✅ **Alertas** - Notificações por email/Slack
- ✅ **Monitoring** - Dashboard de execução

### Qualidade

- ✅ **Unit Tests** - Pytest para código
- ✅ **Integration Tests** - Testes end-to-end
- ✅ **Data Quality** - Validações automatizadas
- ✅ **Logging** - Logs detalhados de cada etapa
- ✅ **Documentation** - Código auto-documentado

## 🛠️ Tecnologias

### Core Stack

- **Python 3.11+** - Linguagem principal
- **Apache Airflow** - Orquestração de workflows
- **PostgreSQL** - Data Warehouse
- **Pandas** - Manipulação de dados
- **SQLAlchemy** - ORM para banco de dados

### Data Processing

- **Pandas** - DataFrames e análise
- **NumPy** - Operações numéricas
- **Great Expectations** - Validação de qualidade

### Infrastructure

- **Docker** - Containerização
- **Docker Compose** - Orquestração local
- **GitHub Actions** - CI/CD

### Testing & Quality

- **Pytest** - Testes unitários
- **Pytest-cov** - Cobertura de código
- **Black** - Formatação de código
- **Flake8** - Linting
- **Pre-commit** - Hooks de commit

## 🚀 Início Rápido

### Pré-requisitos

- Docker & Docker Compose
- Python 3.11+
- Git

### Instalação

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/etl-pipeline-portfolio.git
cd etl-pipeline-portfolio

# 2. Configure o ambiente
cp .env.example .env
# Edite .env com suas credenciais

# 3. Inicie os containers
docker-compose up -d

# 4. Instale as dependências (se rodar local)
python -m venv venv
source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt

# 5. Execute o pipeline
python src/main.py
# ou via Airflow: http://localhost:8080
```

### Acesso às Interfaces

- **Airflow UI**: http://localhost:8080 (airflow/airflow)
- **PostgreSQL**: localhost:5432 (etl_user/etl_password)
- **Logs**: `data/logs/`

## 📁 Estrutura do Projeto

```
etl-pipeline-portfolio/
│
├── src/                          # Código fonte
│   ├── extract/                  # Módulos de extração
│   │   ├── api_extractor.py     # Extração de APIs
│   │   ├── csv_extractor.py     # Extração de CSVs
│   │   └── db_extractor.py      # Extração de DBs
│   │
│   ├── transform/                # Módulos de transformação
│   │   ├── cleaner.py           # Limpeza de dados
│   │   ├── validator.py         # Validação de qualidade
│   │   └── aggregator.py        # Agregações
│   │
│   ├── load/                     # Módulos de carga
│   │   ├── db_loader.py         # Carga no PostgreSQL
│   │   └── strategies.py        # Estratégias de carga
│   │
│   ├── utils/                    # Utilitários
│   │   ├── logger.py            # Sistema de logs
│   │   ├── config.py            # Configurações
│   │   └── db_connection.py    # Conexão com DB
│   │
│   └── main.py                   # Orquestrador principal
│
├── airflow/                      # Apache Airflow
│   └── dags/                     # DAGs
│       ├── etl_daily.py         # Pipeline diário
│       └── etl_weekly.py        # Pipeline semanal
│
├── config/                       # Arquivos de configuração
│   ├── database.yaml            # Config de banco
│   └── pipeline.yaml            # Config do pipeline
│
├── data/                         # Dados
│   ├── raw/                     # Dados brutos
│   ├── processed/               # Dados processados
│   └── logs/                    # Logs de execução
│
├── tests/                        # Testes
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
│
├── notebooks/                    # Jupyter Notebooks
│   └── exploratory_analysis.ipynb
│
├── docs/                         # Documentação
│   ├── architecture.md
│   └── api_documentation.md
│
├── docker/                       # Dockerfiles
│   └── Dockerfile
│
├── .github/workflows/            # CI/CD
│   └── ci.yml
│
├── docker-compose.yml            # Orquestração Docker
├── requirements.txt              # Dependências Python
├── .env.example                  # Exemplo de variáveis
├── Makefile                      # Comandos de automação
└── README.md
```

## 💼 Casos de Uso

### 1. Pipeline de E-commerce

**Fonte**: API de vendas, CSV de produtos  
**Transformação**: Cálculo de métricas (receita, itens vendidos)  
**Destino**: Tabelas fato de vendas e dimensões

### 2. Weather Data Analytics

**Fonte**: OpenWeatherMap API  
**Transformação**: Agregação por cidade, conversão de unidades  
**Destino**: Série temporal de clima

### 3. Customer Data Integration

**Fonte**: Múltiplos CSVs de diferentes sistemas  
**Transformação**: Deduplicação, enriquecimento  
**Destino**: Master Data de clientes

## 📊 Schema do Data Warehouse

```sql
-- Fact Tables
fact_sales (
    sale_id,
    date_id,
    customer_id,
    product_id,
    quantity,
    revenue,
    created_at
)

-- Dimension Tables
dim_date (
    date_id,
    date,
    year,
    month,
    quarter,
    day_of_week
)

dim_customer (
    customer_id,
    name,
    email,
    city,
    state,
    country
)

dim_product (
    product_id,
    name,
    category,
    price,
    cost
)
```

## 🧪 Testes

```bash
# Executar todos os testes
pytest

# Com cobertura
pytest --cov=src --cov-report=html

# Testes específicos
pytest tests/test_extract.py
```

## 📈 Monitoramento

- **Airflow UI**: Visualização de DAGs e execuções
- **Logs**: Arquivo detalhado em `data/logs/`
- **Métricas**: Tempo de execução, linhas processadas
- **Alertas**: Email/Slack em caso de falhas

## 🔧 Configuração

### Variáveis de Ambiente (.env)

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_warehouse
DB_USER=etl_user
DB_PASSWORD=etl_password

# API Keys
WEATHER_API_KEY=your_api_key
```

## 🤝 Contribuindo

Contribuições são bem-vindas! Por favor:

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📝 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

## 👤 Autor

**Seu Nome**
- GitHub: [@seu-usuario](https://github.com/seu-usuario)
- LinkedIn: [Seu Perfil](https://linkedin.com/in/seu-perfil)
- Email: seu.email@exemplo.com

## 🙏 Recursos

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Great Expectations](https://docs.greatexpectations.io/)

---

⭐ **Se este projeto foi útil, considere dar uma estrela!**

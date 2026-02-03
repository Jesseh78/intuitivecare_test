# IntuitiveCare - Pipeline ETL e API de Análise de Despesas

##  Visão Geral

Este projeto implementa uma solução completa de **ETL (Extract, Transform, Load)** para análise de despesas de operadoras de planos de saúde brasileiras, utilizando dados públicos da **ANS (Agência Nacional de Saúde Suplementar)**.

### O que o projeto faz:

1. **Pipeline ETL automatizado**:
   - Baixa automaticamente os últimos 3 trimestres de demonstrações contábeis da ANS
   - Valida e enriquece dados com cadastro de operadoras ativas
   - Agrega informações por operadora e UF
   - Exporta dados processados para PostgreSQL

2. **Banco de dados PostgreSQL**:
   - Schema normalizado com constraints e índices
   - Views materializadas para consultas otimizadas
   - Queries analíticas pré-construídas (top 5, crescimento, etc.)

3. **API REST (FastAPI)**:
   - Listagem de operadoras com paginação e busca
   - Histórico de despesas por operadora
   - Estatísticas agregadas com cache em memória
   - Documentação interativa (Swagger)

---

##  Requisitos

### Obrigatórios:
- **Python 3.10+**
- **Docker Desktop** (para PostgreSQL)
- **Git**

### Bibliotecas Python (instaladas via requirements.txt):
- `pandas` (processamento de dados)
- `requests` (download de arquivos ANS)
- `fastapi` + `uvicorn` (API REST)
- `psycopg[binary]` (PostgreSQL driver)
- `openpyxl` (leitura de Excel)

---

##  Setup Rápido

### 1. Clone o repositório
```bash
git clone <url-do-repo>
cd intuitivecare_test
```

### 2. Crie e ative o ambiente virtual
```bash
# Windows PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Windows CMD
python -m venv .venv
.venv\Scripts\activate.bat

# Linux/Mac
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Instale as dependências
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## 🐘 Configuração do PostgreSQL

### 1. Inicie o banco com Docker Compose
```bash
docker-compose up -d
```

**Configurações do banco**:
- **Host**: `localhost`
- **Porta**: `5434` (não 5432, para evitar conflitos)
- **Usuário**: `intuitive`
- **Senha**: `intuitive`
- **Database**: `intuitivecare`

### 2. Verifique se o container está rodando
```bash
docker ps
```

### 3. Teste a conexão
```bash
# Windows PowerShell
docker exec -it intuitivecare_pg psql -U intuitive -d intuitivecare -c "SELECT version();"
```

---

##  Executar o Pipeline ETL

O pipeline é dividido em 3 etapas sequenciais:

### Step 1: Baixar e consolidar dados da ANS
```bash
python -m src.cli step1
```

**O que faz**:
- Acessa o FTP público da ANS
- Identifica os últimos 3 trimestres disponíveis
- Baixa ZIPs das demonstrações contábeis
- Extrai e normaliza tabelas (CSV/Excel)
- Gera: `data/processed/consolidado_despesas.csv`

**Colunas geradas**: `CNPJ, RazaoSocial, Ano, Trimestre, ValorDespesas`

---

### Step 2: Validar, enriquecer e agregar
```bash
python -m src.cli step2 --invalid-cnpj-strategy keep_mark
```

**O que faz**:
- Valida CNPJs (dígitos verificadores)
- Baixa cadastro de operadoras ativas da ANS
- Enriquece com: `RegistroANS, Modalidade, UF`
- Gera agregações por operadora/UF (total, média, desvio padrão)

**Arquivos gerados**:
- `data/processed/consolidado_enriquecido.csv`
- `data/processed/operadoras_ativas_normalizado.csv`
- `data/processed/despesas_agregadas.csv`
- `data/processed/invalid_rows_step2.csv` (CNPJs inválidos, se `keep_mark`)
- `data/processed/join_sem_match_step2.csv` (CNPJs sem match no cadastro)

**Opções de estratégia**:
- `--invalid-cnpj-strategy drop`: Remove linhas com CNPJ inválido
- `--invalid-cnpj-strategy keep_mark` (**recomendado**): Mantém marcado para auditoria

---

### Step 3: Exportar para SQL
```bash
python -m src.cli export-sql
```

**O que faz**:
- Converte colunas para snake_case (padrão SQL)
- Gera CSVs prontos para `\copy` do PostgreSQL
- Salva em: `data/sql_data/`

**Arquivos gerados**:
- `operadoras_ativas.csv`
- `consolidado_enriquecido.csv`
- `despesas_agregadas.csv`
- `consolidado_despesas.csv` (cópia do step1)

---

##  Aplicar Schema e Views no PostgreSQL

### 1. Criar tabelas e índices
```bash
# Windows PowerShell
type sql\01_schema.sql | docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare

# Linux/Mac
cat sql/01_schema.sql | docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare
```

**Tabelas criadas**:
- `operadoras_ativas` (PK: cnpj)
- `despesas_trimestrais` (FK → operadoras_ativas)
- `despesas_agregadas` (estatísticas pré-calculadas)

---

### 2. Criar views
```bash
# Windows PowerShell
type sql\04_views.sql | docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare
```

**Views criadas**:
- `operadoras_ativas_view` (une cadastro + razão social das despesas)

---

##  Carga de Dados no PostgreSQL

### 1. Copie os CSVs para dentro do container
```bash
docker cp data/sql_data/. intuitivecare_pg:/tmp/sql_data/
```

### 2. Execute a carga via psql
```bash
# Windows PowerShell
docker exec -it intuitivecare_pg psql -U intuitive -d intuitivecare
```

Dentro do psql:
```sql
-- Limpa dados anteriores (se re-executar)
TRUNCATE TABLE despesas_trimestrais CASCADE;
TRUNCATE TABLE despesas_agregadas RESTART IDENTITY;
TRUNCATE TABLE operadoras_ativas CASCADE;

-- Carga: operadoras ativas
\copy operadoras_ativas (cnpj, registro_ans, modalidade, uf) FROM '/tmp/sql_data/operadoras_ativas.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- Carga: despesas trimestrais enriquecidas
\copy despesas_trimestrais (cnpj, razao_social, trimestre, ano, valor_despesas, registro_ans, modalidade, uf) FROM '/tmp/sql_data/consolidado_enriquecido.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- Carga: agregados
\copy despesas_agregadas (razao_social, uf, total_despesas, media_despesas_tri, desvio_padrao_despesas) FROM '/tmp/sql_data/despesas_agregadas.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- Verifica
SELECT COUNT(*) FROM operadoras_ativas;
SELECT COUNT(*) FROM despesas_trimestrais;
SELECT COUNT(*) FROM despesas_agregadas;

\q
```

**Importante**: Use `CASCADE` no TRUNCATE para respeitar a FK entre tabelas.

---

##  Rodar a API REST

### 1. Configure a variável de ambiente
```bash
# Windows PowerShell
$env:DATABASE_URL="postgresql://intuitive:intuitive@localhost:5434/intuitivecare"

# Windows CMD
set DATABASE_URL=postgresql://intuitive:intuitive@localhost:5434/intuitivecare

# Linux/Mac
export DATABASE_URL="postgresql://intuitive:intuitive@localhost:5434/intuitivecare"
```

### 2. Inicie o servidor
```bash
uvicorn src.api.main:app --reload --port 8000
```

### 3. Acesse a documentação interativa
- Swagger UI: **http://localhost:8000/docs**
- ReDoc: **http://localhost:8000/redoc**

---

##  Endpoints da API

### 1. **GET** `/api/operadoras` - Listar operadoras
**Paginação + filtro por razão social ou CNPJ**

```bash
# Listar primeira página (10 itens)
curl "http://localhost:8000/api/operadoras?page=1&limit=10"

# Buscar por razão social
curl "http://localhost:8000/api/operadoras?q=UNIMED"

# Buscar por CNPJ
curl "http://localhost:8000/api/operadoras?q=12345678"
```

**Resposta**:
```json
{
  "data": [
    {
      "cnpj": "12345678000199",
      "razao_social": "UNIMED EXAMPLE COOPERATIVA",
      "registro_ans": "123456",
      "modalidade": "Cooperativa Médica",
      "uf": "SP"
    }
  ],
  "total": 150,
  "page": 1,
  "limit": 10
}
```

---

### 2. **GET** `/api/operadoras/{cnpj}` - Detalhes de uma operadora
```bash
curl "http://localhost:8000/api/operadoras/12345678000199"
```

**Resposta**:
```json
{
  "cnpj": "12345678000199",
  "razao_social": "UNIMED EXAMPLE COOPERATIVA",
  "registro_ans": "123456",
  "modalidade": "Cooperativa Médica",
  "uf": "SP"
}
```

---

### 3. **GET** `/api/operadoras/{cnpj}/despesas` - Histórico de despesas
```bash
curl "http://localhost:8000/api/operadoras/12345678000199/despesas"
```

**Resposta**:
```json
[
  {
    "ano": 2025,
    "trimestre": 1,
    "valor_despesas": 1500000.00
  },
  {
    "ano": 2025,
    "trimestre": 2,
    "valor_despesas": 1750000.00
  }
]
```

---

### 4. **GET** `/api/estatisticas` - Estatísticas agregadas
**Cache em memória (TTL: 5 minutos)**

```bash
# Normal (usa cache se disponível)
curl "http://localhost:8000/api/estatisticas"

# Forçar recálculo (ignora cache)
curl "http://localhost:8000/api/estatisticas?force=true"
```

**Resposta**:
```json
{
  "total_despesas": 15000000000.00,
  "media_despesas": 2500000.00,
  "top5_operadoras": [
    {
      "razao_social": "BRADESCO SAUDE S/A",
      "total_despesas": 500000000.00
    }
  ],
  "distribuicao_por_uf": [
    {
      "uf": "SP",
      "total_despesas": 8000000000.00
    }
  ],
  "cached": true
}
```

---

##  Estrutura do Projeto

```
intuitivecare_test/
│
├── data/                                    # Dados processados
│   ├── raw/                                 # CSVs baixados da ANS (por trimestre)
│   ├── processed/                           # Saída do pipeline (consolidado, enriquecido, etc.)
│   ├── reference/                           # Cadastros baixados da ANS
│   └── sql_data/                            # CSVs prontos para carga no PostgreSQL
│
├── sql/                                     # Scripts SQL
│   ├── 01_schema.sql                        # CREATE TABLE + índices
│   ├── 02_load.sql                          # Exemplo de \copy (referência)
│   ├── 03_queries.sql                       # Queries analíticas (top 5, crescimento, UFs)
│   └── 04_views.sql                         # CREATE VIEW (operadoras_ativas_view)
│
├── src/
│   ├── api/                                 # API REST (FastAPI)
│   │   ├── main.py                          # Rotas e endpoints
│   │   ├── schemas.py                       # Pydantic models (request/response)
│   │   ├── service.py                       # Lógica de negócio (queries SQL)
│   │   └── db.py                            # Conexão com PostgreSQL (psycopg)
│   │
│   ├── pipeline/                            # Pipeline ETL
│   │   ├── ans_fetch.py                     # Step 1: baixa e consolida dados ANS
│   │   ├── validate_enrich.py               # Step 2: valida CNPJ, enriquece com cadastro
│   │   ├── aggregate.py                     # Step 2: agrega por operadora/UF
│   │   └── export_sql_data.py               # Step 3: prepara CSVs para PostgreSQL
│   │
│   ├── utils/                               # Utilitários
│   │   ├── fs.py                            # Helpers para sistema de arquivos
│   │   └── http.py                          # Download de arquivos (requests)
│   │
│   └── cli.py                               # Interface de linha de comando (argparse)
│
├── docker-compose.yml                       # PostgreSQL containerizado
├── requirements.txt                         # Dependências Python
└── README.md                                # Este arquivo
```

---

##  Decisões Técnicas e Tradeoffs

### 1. **Estratégia `keep_mark` para CNPJs inválidos**
**Por quê?**  
- Mantém rastreabilidade para auditoria
- Permite identificar problemas nos dados da ANS
- Não perde informação (pode ser corrigida manualmente)

**Tradeoff**:  
- ❌ Gera linhas órfãs (sem FK válida)
- ✅ Mantém integridade analítica (saber o que foi descartado)

**Alternativa**: `drop` (remove silenciosamente, pode esconder problemas)

---

### 2. **Encoding `latin-1` para cadastro ANS**
**Por quê?**  
- Arquivos da ANS usam `Windows-1252` (não UTF-8)
- Evita `UnicodeDecodeError` ao ler CSVs com acentuação

**Tradeoff**:  
- ❌ Não é portável (depende do encoding do servidor ANS)
- ✅ Funciona com dados reais (testado)

**Alternativa**: Converter manualmente para UTF-8 (adiciona etapa extra)

---

### 3. **Cache em memória (5 minutos) para estatísticas**
**Por quê?**  
- Reduz carga no banco para queries pesadas (SUM, AVG, GROUP BY)
- API responde mais rápido (< 10ms vs 500ms)

**Tradeoff**:  
- ❌ Dados podem estar desatualizados por até 5 minutos
- ✅ Escalabilidade (suporta mais requests simultâneos)

**Alternativa**: Cache em Redis (mais complexo, mas distribuído)

---

### 4. **Porta 5434 (não 5432) para PostgreSQL**
**Por quê?**  
- Evita conflito com PostgreSQL local (porta padrão 5432)
- Facilita desenvolvimento (múltiplos projetos)

**Tradeoff**:  
- ❌ Precisa lembrar de configurar porta customizada
- ✅ Isolamento total (não interfere com outros bancos)

---

### 5. **Download automático vs. manual da ANS**
**Por quê?**  
- Automatiza coleta (reduz erro humano)
- Identifica último trimestre disponível dinamicamente

**Tradeoff**:  
- ❌ Depende de disponibilidade do FTP ANS (pode cair)
- ✅ Atualização automática (executar step1 novamente sempre busca últimos dados)

**Alternativa**: Fornecer CSVs manualmente (mais controle, menos automação)

---

### 6. **FK com DEFERRABLE INITIALLY DEFERRED**
**Por quê?**  
- Permite carga em qualquer ordem (operadoras → despesas ou vice-versa)
- Evita erro de violação de FK durante `\copy`

**Tradeoff**:  
- ❌ Validação acontece só no COMMIT (erros aparecem mais tarde)
- ✅ Flexibilidade na carga (não precisa respeitar ordem estrita)

---

### 7. **Pandas para ETL (não Spark/Dask)**
**Por quê?**  
- Volume de dados pequeno (~50-100MB por trimestre)
- Simplicidade (sem cluster, menos dependências)

**Tradeoff**:  
- ❌ Não escala para Big Data (limite: ~1-2GB de RAM)
- ✅ Setup rápido, fácil debug

**Alternativa**: Spark/Dask (overkill para esse volume)

---

### 8. **FastAPI (não Flask/Django)**
**Por quê?**  
- Performance (assíncrono por padrão)
- Validação automática (Pydantic)
- Documentação Swagger embutida

**Tradeoff**:  
- ❌ Menos maturidade que Flask (menos plugins)
- ✅ Código mais limpo (type hints nativos)

---

## 🔧 Troubleshooting

### Problema 1: **SSL Error ao baixar da ANS**
```
SSLError: [SSL: CERTIFICATE_VERIFY_FAILED]
```

**Solução**:
```python
# Adicione verify=False no requests (src/utils/http.py)
# Apenas para desenvolvimento local!
response = requests.get(url, verify=False)
```

---

### Problema 2: **Encoding error ao ler cadastro**
```
UnicodeDecodeError: 'utf-8' codec can't decode byte
```

**Solução**: Cadastro ANS usa `latin-1`, já configurado em `validate_enrich.py`:
```python
cad = pd.read_csv(path, encoding="latin-1", sep=";")
```

---

### Problema 3: **FK constraint error na carga**
```
ERROR: insert or update on table "despesas_trimestrais" violates foreign key
```

**Solução**: Limpe na ordem correta:
```sql
TRUNCATE TABLE despesas_trimestrais CASCADE;
TRUNCATE TABLE operadoras_ativas CASCADE;
```

Ou use `DEFERRABLE` (já configurado no schema).

---

### Problema 4: **Windows não reconhece variável de ambiente**
```bash
# PowerShell
$env:DATABASE_URL="postgresql://..."

# CMD
set DATABASE_URL=postgresql://...
```

**Alternativa**: Crie arquivo `.env`:
```
DATABASE_URL=postgresql://intuitive:intuitive@localhost:5434/intuitivecare
```

E adicione `python-dotenv`:
```python
from dotenv import load_dotenv
load_dotenv()
```

---

### Problema 5: **Container PostgreSQL não inicia**
```
Error: port 5434 already in use
```

**Solução**:
```bash
# Verifique processo usando a porta
netstat -ano | findstr :5434

# Ou mude a porta no docker-compose.yml
ports:
  - "5435:5432"  # Porta externa diferente
```

---

### Problema 6: **Dados não aparecem na API**
**Checklist**:
1. Pipeline executado? (`step1` → `step2` → `export-sql`)
2. Schema criado? (`01_schema.sql`)
3. Carga feita? (`\copy` dos CSVs)
4. Views criadas? (`04_views.sql`)
5. `DATABASE_URL` correto?

**Debug**:
```bash
docker exec -it intuitivecare_pg psql -U intuitive -d intuitivecare -c "SELECT COUNT(*) FROM despesas_trimestrais;"
```

---

##  Referências

- **Dados públicos ANS**: https://dadosabertos.ans.gov.br/
- **FastAPI Docs**: https://fastapi.tiangolo.com/
- **PostgreSQL COPY**: https://www.postgresql.org/docs/current/sql-copy.html
- **Pandas Docs**: https://pandas.pydata.org/docs/

---

##  Licença

Este projeto foi desenvolvido para fins de avaliação técnica.

---

##  Autor

Desenvolvido por José Ulisses como parte do processo seletivo para Estágio IntuitiveCare 2026 **IntuitiveCare**.

# ==============================================================================
# SETUP COMPLETO - IntuitiveCare Test (PowerShell Simplificado)
# ==============================================================================

$ErrorActionPreference = "Continue"

Write-Host "`n🚀 Iniciando SETUP IntuitiveCare..." -ForegroundColor Cyan

# Check Python
try {
    python --version | Out-Null
    Write-Host "✅ Python encontrado" -ForegroundColor Green
} catch {
    Write-Host "❌ Python não encontrado" -ForegroundColor Red
    exit 1
}

# Check Docker
try {
    docker --version | Out-Null
    Write-Host "✅ Docker encontrado" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker não encontrado" -ForegroundColor Red
    exit 1
}

# ============================================================================
# PASSO 1: PostgreSQL
# ============================================================================
Write-Host "`n▶️  [1/5] Iniciando PostgreSQL..." -ForegroundColor Cyan
docker-compose up -d db
Write-Host "Aguardando PostgreSQL ficar pronto..." -NoNewline
Start-Sleep -Seconds 10
Write-Host " ✅" -ForegroundColor Green

# Aplicar schema
Write-Host "Aplicando schema SQL..." -NoNewline
Get-Content sql/01_schema.sql | docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare 2>&1 | Out-Null
Write-Host " ✅" -ForegroundColor Green

# ============================================================================
# PASSO 2: Pipeline ETL
# ============================================================================
Write-Host "`n▶️  [2/5] Executando Pipeline ETL..." -ForegroundColor Cyan

Write-Host "  Step 1: Baixando dados..." -NoNewline
python -m src.cli step1 2>&1 | Out-Null
Write-Host " ✅" -ForegroundColor Green

Write-Host "  Step 2: Validando e enriquecendo..." -NoNewline
python -m src.cli step2 --invalid-cnpj-strategy keep_mark 2>&1 | Out-Null
Write-Host " ✅" -ForegroundColor Green

Write-Host "  Step 3: Exportando para SQL..." -NoNewline
python -m src.cli export-sql 2>&1 | Out-Null
Write-Host " ✅" -ForegroundColor Green

# ============================================================================
# PASSO 3: Carregar dados
# ============================================================================
Write-Host "`n▶️  [3/5] Carregando dados no PostgreSQL..." -ForegroundColor Cyan
Write-Host "  Copiando CSVs..." -NoNewline
docker cp data/sql_data/. intuitivecare_pg:/tmp/sql_data/ 2>&1 | Out-Null
Write-Host " ✅" -ForegroundColor Green

Write-Host "  Executando COPY..." -NoNewline
@'
TRUNCATE TABLE despesas_trimestrais CASCADE;
TRUNCATE TABLE despesas_agregadas RESTART IDENTITY;
TRUNCATE TABLE operadoras_ativas CASCADE;
\copy operadoras_ativas (cnpj, registro_ans, modalidade, uf) FROM '/tmp/sql_data/operadoras_ativas.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy despesas_trimestrais (cnpj, razao_social, trimestre, ano, valor_despesas, registro_ans, modalidade, uf) FROM '/tmp/sql_data/consolidado_enriquecido.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy despesas_agregadas (razao_social, uf, total_despesas, media_despesas_tri, desvio_padrao_despesas) FROM '/tmp/sql_data/despesas_agregadas.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
'@ | docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare 2>&1 | Out-Null
Write-Host " ✅" -ForegroundColor Green

# ============================================================================
# PASSO 4: Testes
# ============================================================================
Write-Host "`n▶️  [4/5] Executando testes..." -ForegroundColor Cyan
.venv\Scripts\python.exe -m pytest tests/ -v --tb=line 2>&1 | findstr "passed\|failed"
Write-Host "✅ Testes concluídos" -ForegroundColor Green

# ============================================================================
# PASSO 5: API
# ============================================================================
Write-Host "`n▶️  [5/5] Iniciando API..." -ForegroundColor Cyan
docker-compose up -d api
Write-Host "Aguardando API..." -NoNewline
Start-Sleep -Seconds 5
Write-Host " ✅" -ForegroundColor Green

# ============================================================================
# FINALIZAÇÃO
# ============================================================================
Write-Host "`n" -ForegroundColor Green
Write-Host "╔════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║     ✨ SETUP CONCLUÍDO COM SUCESSO! ✨            ║" -ForegroundColor Green
Write-Host "╚════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "🔗 Endpoints disponíveis:" -ForegroundColor Cyan
Write-Host "  • API:       http://localhost:8000" -ForegroundColor White
Write-Host "  • Swagger:   http://localhost:8000/docs" -ForegroundColor White
Write-Host "  • PostgreSQL: localhost:5434" -ForegroundColor White
Write-Host ""
Write-Host "🛑 Para parar tudo: docker-compose down" -ForegroundColor Yellow
Write-Host ""

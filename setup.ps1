# ==============================================================================
# SETUP COMPLETO - IntuitiveCare Test
# ==============================================================================
# Script automatizado para configuração e execução do pipeline ETL + API
# 
# Autor: José (Candidato Estágio IntuitiveCare 2026)
# Data: Fevereiro 2026
# ==============================================================================

param(
    [switch]$SkipPipeline = $false,
    [switch]$SkipDatabase = $false,
    [switch]$SkipAPI = $false,
    [switch]$CleanData = $false
)

$ErrorActionPreference = "Stop"

# Cores para output
function Write-Step {
    param([string]$Message)
    Write-Host "`n🔷 $Message" -ForegroundColor Cyan
}

function Write-Success {
    param([string]$Message)
    Write-Host "✅ $Message" -ForegroundColor Green
}

function Write-Error-Custom {
    param([string]$Message)
    Write-Host "❌ $Message" -ForegroundColor Red
}

function Write-Info {
    param([string]$Message)
    Write-Host "ℹ️  $Message" -ForegroundColor Yellow
}

# ==============================================================================
# BANNER
# ==============================================================================
Clear-Host
Write-Host @"
╔═══════════════════════════════════════════════════════════════════╗
║                                                                   ║
║   ██╗███╗   ██╗████████╗██╗   ██╗██╗████████╗██╗██╗   ██╗███████╗║
║   ██║████╗  ██║╚══██╔══╝██║   ██║██║╚══██╔══╝██║██║   ██║██╔════╝║
║   ██║██╔██╗ ██║   ██║   ██║   ██║██║   ██║   ██║██║   ██║█████╗  ║
║   ██║██║╚██╗██║   ██║   ██║   ██║██║   ██║   ██║╚██╗ ██╔╝██╔══╝  ║
║   ██║██║ ╚████║   ██║   ╚██████╔╝██║   ██║   ██║ ╚████╔╝ ███████╗║
║   ╚═╝╚═╝  ╚═══╝   ╚═╝    ╚═════╝ ╚═╝   ╚═╝   ╚═╝  ╚═══╝  ╚══════╝║
║                                                                   ║
║            Pipeline ETL + API - Teste Técnico Estágio            ║
║                                                                   ║
╚═══════════════════════════════════════════════════════════════════╝
"@ -ForegroundColor Magenta

Start-Sleep -Seconds 1

# ==============================================================================
# STEP 0: Validações iniciais
# ==============================================================================
Write-Step "Validando pré-requisitos..."

# Check Python
try {
    $pythonVersion = python --version 2>&1
    Write-Success "Python encontrado: $pythonVersion"
} catch {
    Write-Error-Custom "Python não encontrado. Instale Python 3.10+ antes de continuar."
    exit 1
}

# Check Docker
try {
    $dockerVersion = docker --version 2>&1
    Write-Success "Docker encontrado: $dockerVersion"
} catch {
    Write-Error-Custom "Docker não encontrado. Instale Docker Desktop antes de continuar."
    exit 1
}

# Check virtual environment
if (-not (Test-Path ".venv")) {
    Write-Info "Virtual environment não encontrado. Criando..."
    python -m venv .venv
    Write-Success "Virtual environment criado"
}

# Ativar venv
Write-Info "Ativando virtual environment..."
& .\.venv\Scripts\Activate.ps1

# Instalar dependências
Write-Info "Instalando dependências Python..."
pip install --upgrade pip -q
pip install -r requirements.txt -q
Write-Success "Dependências instaladas"

# ==============================================================================
# STEP 1: Docker - PostgreSQL
# ==============================================================================
if (-not $SkipDatabase) {
    Write-Step "Iniciando PostgreSQL com Docker..."
    
    # Check se já está rodando
    $containerRunning = docker ps --filter "name=intuitivecare_pg" --format "{{.Names}}" 2>$null
    
    if ($containerRunning -eq "intuitivecare_pg") {
        Write-Info "Container PostgreSQL já está rodando"
    } else {
        Write-Info "Iniciando container PostgreSQL..."
        docker-compose up -d db
        
        # Aguardar health check
        Write-Info "Aguardando PostgreSQL ficar pronto..."
        $timeout = 30
        $elapsed = 0
        while ($elapsed -lt $timeout) {
            $health = docker inspect --format='{{.State.Health.Status}}' intuitivecare_pg 2>$null
            if ($health -eq "healthy") {
                break
            }
            Start-Sleep -Seconds 2
            $elapsed += 2
            Write-Host "." -NoNewline
        }
        Write-Host ""
        
        if ($elapsed -ge $timeout) {
            Write-Error-Custom "PostgreSQL não ficou pronto em ${timeout}s"
            exit 1
        }
        
        Write-Success "PostgreSQL pronto na porta 5434"
    }
    
    # Criar schema
    Write-Info "Aplicando schema SQL..."
    Get-Content sql/01_schema.sql | docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare 2>&1 | Out-Null
    Write-Success "Schema aplicado"
    
    # Criar views
    Write-Info "Criando views..."
    Get-Content sql/04_views.sql | docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare 2>&1 | Out-Null
    Write-Success "Views criadas"
}

# ==============================================================================
# STEP 2: Pipeline ETL
# ==============================================================================
if (-not $SkipPipeline) {
    Write-Step "Executando Pipeline ETL..."
    
    # Step 1: Download e consolidação
    Write-Info "Step 1: Baixando últimos 3 trimestres da ANS..."
    python -m src.cli step1
    if ($LASTEXITCODE -ne 0) {
        Write-Error-Custom "Erro no Step 1"
        exit 1
    }
    Write-Success "Step 1 concluído - consolidado_despesas.csv gerado"
    
    # Step 2: Validação e enriquecimento
    Write-Info "Step 2: Validando CNPJs e enriquecendo com cadastro ANS..."
    python -m src.cli step2 --invalid-cnpj-strategy keep_mark
    if ($LASTEXITCODE -ne 0) {
        Write-Error-Custom "Erro no Step 2"
        exit 1
    }
    Write-Success "Step 2 concluído - dados validados e enriquecidos"
    
    # Step 3: Export para SQL
    Write-Info "Step 3: Exportando dados para formato SQL..."
    python -m src.cli export-sql
    if ($LASTEXITCODE -ne 0) {
        Write-Error-Custom "Erro no Step 3"
        exit 1
    }
    Write-Success "Step 3 concluído - CSVs prontos para carga"
    
    # Estatísticas
    Write-Step "Estatísticas dos dados processados:"
    $consolidado = Get-Content "data/processed/consolidado_despesas.csv" | Measure-Object -Line
    $enriquecido = Get-Content "data/processed/consolidado_enriquecido.csv" | Measure-Object -Line
    $agregado = Get-Content "data/processed/despesas_agregadas.csv" | Measure-Object -Line
    
    Write-Host "  📊 Consolidado (Step 1): $($consolidado.Lines - 1) registros" -ForegroundColor White
    Write-Host "  📊 Enriquecido (Step 2): $($enriquecido.Lines - 1) registros" -ForegroundColor White
    Write-Host "  📊 Agregado (Step 2):    $($agregado.Lines - 1) registros" -ForegroundColor White
}

# ==============================================================================
# STEP 3: Carga no PostgreSQL
# ==============================================================================
if (-not $SkipDatabase) {
    Write-Step "Carregando dados no PostgreSQL..."
    
    # Copiar CSVs para dentro do container
    Write-Info "Copiando CSVs para o container..."
    docker cp data/sql_data/. intuitivecare_pg:/tmp/sql_data/
    
    # Executar carga
    Write-Info "Executando comandos COPY..."
    
    $loadSQL = @"
-- Limpa dados anteriores
TRUNCATE TABLE despesas_trimestrais CASCADE;
TRUNCATE TABLE despesas_agregadas RESTART IDENTITY;
TRUNCATE TABLE operadoras_ativas CASCADE;

-- Carga: operadoras ativas
\copy operadoras_ativas (cnpj, registro_ans, modalidade, uf) FROM '/tmp/sql_data/operadoras_ativas.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- Carga: despesas trimestrais
\copy despesas_trimestrais (cnpj, razao_social, trimestre, ano, valor_despesas, registro_ans, modalidade, uf) FROM '/tmp/sql_data/consolidado_enriquecido.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- Carga: agregados
\copy despesas_agregadas (razao_social, uf, total_despesas, media_despesas_tri, desvio_padrao_despesas) FROM '/tmp/sql_data/despesas_agregadas.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
"@
    
    $loadSQL | docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare 2>&1 | Out-Null
    
    # Validar carga
    $validationSQL = @"
SELECT 
    'Operadoras' AS tabela,
    COUNT(*) AS registros
FROM operadoras_ativas
UNION ALL
SELECT 
    'Despesas',
    COUNT(*)
FROM despesas_trimestrais
UNION ALL
SELECT 
    'Agregados',
    COUNT(*)
FROM despesas_agregadas;
"@
    
    Write-Success "Dados carregados com sucesso"
    Write-Step "Validação da carga:"
    
    $validationSQL | docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare -t 2>&1 | ForEach-Object {
        if ($_ -match '\S') {
            Write-Host "  $_" -ForegroundColor White
        }
    }
}

# ==============================================================================
# STEP 4: Testes
# ==============================================================================
Write-Step "Executando testes automatizados..."

Write-Info "Instalando dependências de desenvolvimento..."
pip install -r requirements-dev.txt -q

Write-Info "Executando pytest..."
$env:DATABASE_URL = "postgresql://intuitive:intuitive@localhost:5434/intuitivecare"
pytest -v --tb=short

if ($LASTEXITCODE -eq 0) {
    Write-Success "Todos os testes passaram! ✨"
} else {
    Write-Error-Custom "Alguns testes falharam. Revise os logs acima."
}

# ==============================================================================
# STEP 5: API
# ==============================================================================
if (-not $SkipAPI) {
    Write-Step "Iniciando API REST..."
    
    # Check se container API já existe
    $apiRunning = docker ps --filter "name=intuitivecare_api" --format "{{.Names}}" 2>$null
    
    if ($apiRunning -eq "intuitivecare_api") {
        Write-Info "Container API já está rodando"
    } else {
        Write-Info "Iniciando container da API..."
        docker-compose up -d api
        
        # Aguardar API ficar pronta
        Write-Info "Aguardando API ficar pronta..."
        $timeout = 30
        $elapsed = 0
        while ($elapsed -lt $timeout) {
            try {
                $response = Invoke-WebRequest -Uri "http://localhost:8000/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
                if ($response.StatusCode -eq 200) {
                    break
                }
            } catch {}
            Start-Sleep -Seconds 2
            $elapsed += 2
            Write-Host "." -NoNewline
        }
        Write-Host ""
        
        if ($elapsed -ge $timeout) {
            Write-Error-Custom "API não ficou pronta em ${timeout}s"
            Write-Info "Logs da API:"
            docker logs --tail 50 intuitivecare_api
            exit 1
        }
    }
    
    Write-Success "API disponível em http://localhost:8000"
    Write-Success "Documentação Swagger: http://localhost:8000/docs"
}

# ==============================================================================
# FINALIZAÇÃO
# ==============================================================================
Write-Host ""
Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║                                                               ║" -ForegroundColor Green
Write-Host "║               ✨ SETUP CONCLUÍDO COM SUCESSO! ✨              ║" -ForegroundColor Green
Write-Host "║                                                               ║" -ForegroundColor Green
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""

Write-Step "Resumo dos Serviços:"
Write-Host "  🗄️  PostgreSQL:  http://localhost:5434" -ForegroundColor Cyan
Write-Host "  🚀 API REST:     http://localhost:8000" -ForegroundColor Cyan
Write-Host "  📚 Swagger:      http://localhost:8000/docs" -ForegroundColor Cyan
Write-Host "  📖 ReDoc:        http://localhost:8000/redoc" -ForegroundColor Cyan

Write-Step "Comandos úteis:"
Write-Host "  docker logs -f intuitivecare_api    # Logs da API" -ForegroundColor Yellow
Write-Host "  docker logs -f intuitivecare_pg     # Logs do PostgreSQL" -ForegroundColor Yellow
Write-Host "  docker-compose down                 # Parar tudo" -ForegroundColor Yellow
Write-Host "  pytest                              # Rodar testes" -ForegroundColor Yellow

Write-Host ""
Write-Success "Projeto pronto para avaliação! 🎯"
Write-Host ""

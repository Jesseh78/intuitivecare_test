#!/bin/bash
# ==============================================================================
# SETUP COMPLETO - IntuitiveCare Test (Linux/Mac)
# ==============================================================================
# Script automatizado para configuração e execução do pipeline ETL + API
# 
# Uso: ./setup.sh [opções]
#   --skip-pipeline   Pula execução do pipeline ETL
#   --skip-database   Pula configuração do banco
#   --skip-api        Não inicia a API
#   --clean           Limpa dados anteriores
# ==============================================================================

set -e  # Exit on error

# Cores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Flags
SKIP_PIPELINE=false
SKIP_DATABASE=false
SKIP_API=false
CLEAN_DATA=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-pipeline) SKIP_PIPELINE=true; shift ;;
        --skip-database) SKIP_DATABASE=true; shift ;;
        --skip-api) SKIP_API=true; shift ;;
        --clean) CLEAN_DATA=true; shift ;;
        *) echo "Opção desconhecida: $1"; exit 1 ;;
    esac
done

# Helper functions
step() {
    echo -e "\n${CYAN}🔷 $1${NC}"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

error() {
    echo -e "${RED}❌ $1${NC}"
}

info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

# ==============================================================================
# BANNER
# ==============================================================================
clear
echo -e "${MAGENTA}"
cat << "EOF"
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
EOF
echo -e "${NC}"

sleep 1

# ==============================================================================
# STEP 0: Validações iniciais
# ==============================================================================
step "Validando pré-requisitos..."

# Check Python
if ! command -v python3 &> /dev/null; then
    error "Python3 não encontrado. Instale Python 3.10+ antes de continuar."
    exit 1
fi
PYTHON_VERSION=$(python3 --version)
success "Python encontrado: $PYTHON_VERSION"

# Check Docker
if ! command -v docker &> /dev/null; then
    error "Docker não encontrado. Instale Docker antes de continuar."
    exit 1
fi
DOCKER_VERSION=$(docker --version)
success "Docker encontrado: $DOCKER_VERSION"

# Virtual environment
if [ ! -d ".venv" ]; then
    info "Virtual environment não encontrado. Criando..."
    python3 -m venv .venv
    success "Virtual environment criado"
fi

# Ativar venv
info "Ativando virtual environment..."
source .venv/bin/activate

# Instalar dependências
info "Instalando dependências Python..."
pip install --upgrade pip -q
pip install -r requirements.txt -q
success "Dependências instaladas"

# ==============================================================================
# STEP 1: Docker - PostgreSQL
# ==============================================================================
if [ "$SKIP_DATABASE" = false ]; then
    step "Iniciando PostgreSQL com Docker..."
    
    # Check se já está rodando
    if docker ps --filter "name=intuitivecare_pg" --format "{{.Names}}" | grep -q "intuitivecare_pg"; then
        info "Container PostgreSQL já está rodando"
    else
        info "Iniciando container PostgreSQL..."
        docker-compose up -d db
        
        # Aguardar health check
        info "Aguardando PostgreSQL ficar pronto..."
        timeout=30
        elapsed=0
        while [ $elapsed -lt $timeout ]; do
            health=$(docker inspect --format='{{.State.Health.Status}}' intuitivecare_pg 2>/dev/null || echo "")
            if [ "$health" = "healthy" ]; then
                break
            fi
            sleep 2
            elapsed=$((elapsed + 2))
            echo -n "."
        done
        echo ""
        
        if [ $elapsed -ge $timeout ]; then
            error "PostgreSQL não ficou pronto em ${timeout}s"
            exit 1
        fi
        
        success "PostgreSQL pronto na porta 5434"
    fi
    
    # Criar schema
    info "Aplicando schema SQL..."
    cat sql/01_schema.sql | docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare > /dev/null 2>&1
    success "Schema aplicado"
    
    # Criar views
    info "Criando views..."
    cat sql/04_views.sql | docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare > /dev/null 2>&1
    success "Views criadas"
fi

# ==============================================================================
# STEP 2: Pipeline ETL
# ==============================================================================
if [ "$SKIP_PIPELINE" = false ]; then
    step "Executando Pipeline ETL..."
    
    # Step 1
    info "Step 1: Baixando últimos 3 trimestres da ANS..."
    python -m src.cli step1
    success "Step 1 concluído - consolidado_despesas.csv gerado"
    
    # Step 2
    info "Step 2: Validando CNPJs e enriquecendo com cadastro ANS..."
    python -m src.cli step2 --invalid-cnpj-strategy keep_mark
    success "Step 2 concluído - dados validados e enriquecidos"
    
    # Step 3
    info "Step 3: Exportando dados para formato SQL..."
    python -m src.cli export-sql
    success "Step 3 concluído - CSVs prontos para carga"
    
    # Estatísticas
    step "Estatísticas dos dados processados:"
    consolidado=$(wc -l < data/processed/consolidado_despesas.csv)
    enriquecido=$(wc -l < data/processed/consolidado_enriquecido.csv)
    agregado=$(wc -l < data/processed/despesas_agregadas.csv)
    
    echo "  📊 Consolidado (Step 1): $((consolidado - 1)) registros"
    echo "  📊 Enriquecido (Step 2): $((enriquecido - 1)) registros"
    echo "  📊 Agregado (Step 2):    $((agregado - 1)) registros"
fi

# ==============================================================================
# STEP 3: Carga no PostgreSQL
# ==============================================================================
if [ "$SKIP_DATABASE" = false ]; then
    step "Carregando dados no PostgreSQL..."
    
    # Copiar CSVs
    info "Copiando CSVs para o container..."
    docker cp data/sql_data/. intuitivecare_pg:/tmp/sql_data/
    
    # Executar carga
    info "Executando comandos COPY..."
    
    docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare << 'EOF' > /dev/null 2>&1
-- Limpa dados anteriores
TRUNCATE TABLE despesas_trimestrais CASCADE;
TRUNCATE TABLE despesas_agregadas RESTART IDENTITY;
TRUNCATE TABLE operadoras_ativas CASCADE;

-- Carga
\copy operadoras_ativas (cnpj, registro_ans, modalidade, uf) FROM '/tmp/sql_data/operadoras_ativas.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy despesas_trimestrais (cnpj, razao_social, trimestre, ano, valor_despesas, registro_ans, modalidade, uf) FROM '/tmp/sql_data/consolidado_enriquecido.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy despesas_agregadas (razao_social, uf, total_despesas, media_despesas_tri, desvio_padrao_despesas) FROM '/tmp/sql_data/despesas_agregadas.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
EOF
    
    success "Dados carregados com sucesso"
    
    step "Validação da carga:"
    docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare -t << 'EOF'
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
EOF
fi

# ==============================================================================
# STEP 4: Testes
# ==============================================================================
step "Executando testes automatizados..."

info "Instalando dependências de desenvolvimento..."
pip install -r requirements-dev.txt -q

info "Executando pytest..."
export DATABASE_URL="postgresql://intuitive:intuitive@localhost:5434/intuitivecare"
pytest -v --tb=short

if [ $? -eq 0 ]; then
    success "Todos os testes passaram! ✨"
else
    error "Alguns testes falharam. Revise os logs acima."
fi

# ==============================================================================
# STEP 5: API
# ==============================================================================
if [ "$SKIP_API" = false ]; then
    step "Iniciando API REST..."
    
    if docker ps --filter "name=intuitivecare_api" --format "{{.Names}}" | grep -q "intuitivecare_api"; then
        info "Container API já está rodando"
    else
        info "Iniciando container da API..."
        docker-compose up -d api
        
        # Aguardar API
        info "Aguardando API ficar pronta..."
        timeout=30
        elapsed=0
        while [ $elapsed -lt $timeout ]; do
            if curl -s http://localhost:8000/health > /dev/null 2>&1; then
                break
            fi
            sleep 2
            elapsed=$((elapsed + 2))
            echo -n "."
        done
        echo ""
        
        if [ $elapsed -ge $timeout ]; then
            error "API não ficou pronta em ${timeout}s"
            docker logs --tail 50 intuitivecare_api
            exit 1
        fi
    fi
    
    success "API disponível em http://localhost:8000"
    success "Documentação Swagger: http://localhost:8000/docs"
fi

# ==============================================================================
# FINALIZAÇÃO
# ==============================================================================
echo ""
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                                                               ║${NC}"
echo -e "${GREEN}║               ✨ SETUP CONCLUÍDO COM SUCESSO! ✨              ║${NC}"
echo -e "${GREEN}║                                                               ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

step "Resumo dos Serviços:"
echo -e "  ${CYAN}🗄️  PostgreSQL:  http://localhost:5434${NC}"
echo -e "  ${CYAN}🚀 API REST:     http://localhost:8000${NC}"
echo -e "  ${CYAN}📚 Swagger:      http://localhost:8000/docs${NC}"
echo -e "  ${CYAN}📖 ReDoc:        http://localhost:8000/redoc${NC}"

step "Comandos úteis:"
echo -e "  ${YELLOW}docker logs -f intuitivecare_api    # Logs da API${NC}"
echo -e "  ${YELLOW}docker logs -f intuitivecare_pg     # Logs do PostgreSQL${NC}"
echo -e "  ${YELLOW}docker-compose down                 # Parar tudo${NC}"
echo -e "  ${YELLOW}pytest                              # Rodar testes${NC}"

echo ""
success "Projeto pronto para avaliação! 🎯"
echo ""

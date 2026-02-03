from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.api.db import load_db_config, get_conn
from src.api.schemas import PaginatedResponse, OperadoraOut, DespesaOut, EstatisticasOut
from src.api.service import (
    list_operadoras,
    get_operadora,
    get_historico_despesas,
    get_estatisticas,
)

# ========================================
# Configuração de Logging
# ========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# ========================================
# Lifecycle: Startup e Shutdown
# ========================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gerencia startup e shutdown da aplicação"""
    # Startup
    logger.info("🚀 Iniciando API IntuitiveCare...")
    
    try:
        # Testa conexão com banco
        load_db_config()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                logger.info(f"✅ Conectado ao PostgreSQL: {version[:50]}...")
    except Exception as e:
        logger.error(f"❌ Erro ao conectar ao banco de dados: {e}")
        logger.error("Verifique se DATABASE_URL está configurado corretamente")
        raise
    
    logger.info("✅ API pronta para receber requisições")
    
    yield
    
    # Shutdown
    logger.info("🛑 Encerrando API IntuitiveCare...")


# ========================================
# Aplicação FastAPI
# ========================================

app = FastAPI(
    title="IntuitiveCare Test API",
    version="1.0.0",
    description="API REST para análise de despesas de operadoras de planos de saúde",
    lifespan=lifespan
)

# CORS liberado para desenvolvimento
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ========================================
# Exception Handlers Globais
# ========================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handler customizado para HTTPException"""
    logger.warning(f"HTTPException {exc.status_code}: {exc.detail} - {request.url}")
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "status_code": exc.status_code}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handler global para exceções não tratadas"""
    logger.error(f"Erro não tratado: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Erro interno do servidor. Verifique os logs para mais detalhes.",
            "status_code": 500
        }
    )


# ========================================
# Endpoints
# ========================================

@app.get("/", include_in_schema=False)
def root():
    """Rota raiz - redireciona para documentação"""
    return {
        "message": "API IntuitiveCare - Análise de Despesas ANS",
        "docs": "/docs",
        "redoc": "/redoc"
    }


@app.get("/health", include_in_schema=False)
def health_check():
    """Health check para containers e load balancers"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        logger.error(f"Health check falhou: {e}")
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "database": "disconnected", "error": str(e)}
        )


@app.get("/api/operadoras", response_model=PaginatedResponse)
def api_list_operadoras(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    q: str | None = Query(None, description="Filtro por razão social ou CNPJ"),
):
    """Lista operadoras com paginação e filtro opcional"""
    logger.info(f"GET /api/operadoras - page={page}, limit={limit}, q={q}")
    try:
        data, total = list_operadoras(page=page, limit=limit, q=q)
        return {"data": data, "total": total, "page": page, "limit": limit}
    except Exception as e:
        logger.error(f"Erro ao listar operadoras: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erro ao buscar operadoras")


@app.get("/api/operadoras/{cnpj}", response_model=OperadoraOut)
def api_get_operadora(cnpj: str):
    """Retorna detalhes de uma operadora específica"""
    logger.info(f"GET /api/operadoras/{cnpj}")
    try:
        op = get_operadora(cnpj)
        if not op:
            raise HTTPException(status_code=404, detail=f"Operadora com CNPJ {cnpj} não encontrada")
        return op
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar operadora {cnpj}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erro ao buscar operadora")


@app.get("/api/operadoras/{cnpj}/despesas", response_model=list[DespesaOut])
def api_get_despesas(cnpj: str):
    """Retorna histórico de despesas trimestrais de uma operadora"""
    logger.info(f"GET /api/operadoras/{cnpj}/despesas")
    try:
        op = get_operadora(cnpj)
        if not op:
            raise HTTPException(status_code=404, detail=f"Operadora com CNPJ {cnpj} não encontrada")
        despesas = get_historico_despesas(cnpj)
        logger.info(f"Retornando {len(despesas)} registros de despesas para {cnpj}")
        return despesas
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar despesas de {cnpj}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erro ao buscar histórico de despesas")


@app.get("/api/estatisticas", response_model=EstatisticasOut)
def api_estatisticas(force: bool = Query(False, description="Força recálculo ignorando cache")):
    """Retorna estatísticas agregadas (total, média, top 5 operadoras, distribuição por UF)"""
    logger.info(f"GET /api/estatisticas - force_refresh={force}")
    try:
        payload, cached = get_estatisticas(force_refresh=force)
        payload["cached"] = cached
        
        if cached:
            logger.info("Estatísticas retornadas do cache")
        else:
            logger.info("Estatísticas recalculadas")
        
        return payload
    except Exception as e:
        logger.error(f"Erro ao buscar estatísticas: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Erro ao calcular estatísticas")

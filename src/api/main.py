from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from src.api.schemas import PaginatedResponse, OperadoraOut, DespesaOut, EstatisticasOut
from src.api.service import (
    list_operadoras,
    get_operadora,
    get_historico_despesas,
    get_estatisticas,
)

app = FastAPI(title="IntuitiveCare Test API", version="1.0.0")

# CORS liberado para o frontend local (Vue)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/api/operadoras", response_model=PaginatedResponse)
def api_list_operadoras(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    q: str | None = Query(None, description="Filtro por razão social ou CNPJ"),
):
    # Rota pedida com paginação page/limit :contentReference[oaicite:5]{index=5}
    data, total = list_operadoras(page=page, limit=limit, q=q)
    return {"data": data, "total": total, "page": page, "limit": limit}


@app.get("/api/operadoras/{cnpj}", response_model=OperadoraOut)
def api_get_operadora(cnpj: str):
    # Rota pedida: detalhes da operadora :contentReference[oaicite:6]{index=6}
    op = get_operadora(cnpj)
    if not op:
        raise HTTPException(status_code=404, detail="Operadora não encontrada")
    return op


@app.get("/api/operadoras/{cnpj}/despesas", response_model=list[DespesaOut])
def api_get_despesas(cnpj: str):
    # Rota pedida: histórico de despesas :contentReference[oaicite:7]{index=7}
    op = get_operadora(cnpj)
    if not op:
        raise HTTPException(status_code=404, detail="Operadora não encontrada")
    return get_historico_despesas(cnpj)


@app.get("/api/estatisticas", response_model=EstatisticasOut)
def api_estatisticas(force: bool = Query(False, description="Força recálculo ignorando cache")):
    # Rota pedida: estatísticas (total, média, top 5) :contentReference[oaicite:8]{index=8}
    payload, cached = get_estatisticas(force_refresh=force)
    payload["cached"] = cached
    return payload

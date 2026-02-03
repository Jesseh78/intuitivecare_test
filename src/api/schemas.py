from __future__ import annotations

from pydantic import BaseModel


class OperadoraOut(BaseModel):
    cnpj: str
    razao_social: str
    registro_ans: str | None = None
    modalidade: str | None = None
    uf: str | None = None


class PaginatedResponse(BaseModel):
    data: list[OperadoraOut]
    total: int
    page: int
    limit: int


class DespesaOut(BaseModel):
    ano: int
    trimestre: int
    valor_despesas: float


class EstatisticasOut(BaseModel):
    total_despesas: float
    media_despesas: float
    top5_operadoras: list[dict]
    distribuicao_por_uf: list[dict]
    cached: bool

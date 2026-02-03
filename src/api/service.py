from __future__ import annotations

import logging
import time
from typing import Any

from src.api.db import get_conn

logger = logging.getLogger(__name__)

# Cache simples em memória para /api/estatisticas
_STATS_CACHE: dict[str, Any] = {"ts": 0.0, "payload": None}
_STATS_TTL_SECONDS = 300  # 5 minutos


def list_operadoras(page: int, limit: int, q: str | None = None) -> tuple[list[dict], int]:
    """
    Lista operadoras com paginação (offset-based) e filtro opcional por:
      - razão social (ILIKE)
      - CNPJ (match por substring)
    """
    offset = (page - 1) * limit
    q = (q or "").strip()

    where = ""
    params: list[Any] = []
    if q:
        where = "WHERE razao_social ILIKE %s OR cnpj LIKE %s"
        params.extend([f"%{q}%", f"%{q.replace('.', '').replace('/', '').replace('-', '')}%"])

    count_sql = f"SELECT COUNT(*) FROM operadoras_ativas_view {where}"
    data_sql = f"""
        SELECT cnpj, razao_social, registro_ans, modalidade, uf
        FROM operadoras_ativas_view
        {where}
        ORDER BY razao_social ASC
        LIMIT %s OFFSET %s
    """

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                total = int(cur.fetchone()[0])

                cur.execute(data_sql, params + [limit, offset])
                rows = cur.fetchall()

        data = [
            {
                "cnpj": r[0],
                "razao_social": r[1],
                "registro_ans": r[2],
                "modalidade": r[3],
                "uf": r[4],
            }
            for r in rows
        ]
        logger.debug(f"Listagem: {len(data)} operadoras retornadas (total: {total})")
        return data, total
    except Exception as e:
        logger.error(f"Erro ao listar operadoras: {e}", exc_info=True)
        raise


def get_operadora(cnpj: str) -> dict | None:
    """Retorna detalhes de uma operadora específica."""
    cnpj_digits = "".join(ch for ch in cnpj if ch.isdigit())
    sql = """
        SELECT cnpj, razao_social, registro_ans, modalidade, uf
        FROM operadoras_ativas_view
        WHERE cnpj = %s
        LIMIT 1
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [cnpj_digits])
                r = cur.fetchone()
        
        if not r:
            logger.debug(f"Operadora {cnpj_digits} não encontrada")
            return None
        
        return {
            "cnpj": r[0],
            "razao_social": r[1],
            "registro_ans": r[2],
            "modalidade": r[3],
            "uf": r[4],
        }
    except Exception as e:
        logger.error(f"Erro ao buscar operadora {cnpj_digits}: {e}", exc_info=True)
        raise


def get_historico_despesas(cnpj: str) -> list[dict]:
    """Histórico de despesas por trimestre para uma operadora."""
    cnpj_digits = "".join(ch for ch in cnpj if ch.isdigit())
    sql = """
        SELECT ano, trimestre, SUM(valor_despesas) AS total
        FROM despesas_trimestrais
        WHERE cnpj = %s
        GROUP BY ano, trimestre
        ORDER BY ano ASC, trimestre ASC
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [cnpj_digits])
                rows = cur.fetchall()

        despesas = [{"ano": int(r[0]), "trimestre": int(r[1]), "valor_despesas": float(r[2])} for r in rows]
        logger.debug(f"Histórico: {len(despesas)} registros para CNPJ {cnpj_digits}")
        return despesas
    except Exception as e:
        logger.error(f"Erro ao buscar histórico de despesas para {cnpj_digits}: {e}", exc_info=True)
        raise


def get_estatisticas(force_refresh: bool = False) -> tuple[dict, bool]:
    """
    Estatísticas agregadas:
      - total geral de despesas
      - média geral
      - top 5 operadoras por total
      - distribuição por UF (total)
    Cache: TTL em memória para reduzir carga.
    """
    now = time.time()
    cached_payload = _STATS_CACHE.get("payload")
    cached_ts = float(_STATS_CACHE.get("ts") or 0.0)

    if (not force_refresh) and cached_payload and (now - cached_ts) < _STATS_TTL_SECONDS:
        logger.debug("Estatísticas retornadas do cache")
        return cached_payload, True

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COALESCE(SUM(valor_despesas),0), COALESCE(AVG(valor_despesas),0) FROM despesas_trimestrais")
                total, media = cur.fetchone()

                cur.execute("""
                    SELECT razao_social, COALESCE(SUM(valor_despesas),0) AS total
                    FROM despesas_trimestrais
                    GROUP BY razao_social
                    ORDER BY total DESC
                    LIMIT 5
                """)
                top5 = [{"razao_social": r[0], "total_despesas": float(r[1])} for r in cur.fetchall()]

                cur.execute("""
                    SELECT uf, COALESCE(SUM(valor_despesas),0) AS total
                    FROM despesas_trimestrais
                    GROUP BY uf
                ORDER BY total DESC
            """)
            por_uf = [{"uf": r[0], "total_despesas": float(r[1])} for r in cur.fetchall()]

        payload = {
            "total_despesas": float(total),
            "media_despesas": float(media),
            "top5_operadoras": top5,
            "distribuicao_por_uf": por_uf,
        }
        _STATS_CACHE["ts"] = now
        _STATS_CACHE["payload"] = payload
        
        logger.info(f"Estatísticas recalculadas: total={total:.2f}, top5={len(top5)}, ufs={len(por_uf)}")
        return payload, False
    except Exception as e:
        logger.error(f"Erro ao calcular estatísticas: {e}", exc_info=True)
        raise

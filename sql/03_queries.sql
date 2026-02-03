-- 03_queries.sql

-- Q1) Top 5 operadoras por crescimento percentual (primeiro -> último trimestre disponível)
WITH base AS (
  SELECT
    cnpj,
    razao_social,
    (ano * 10 + trimestre) AS periodo_key,
    SUM(valor_despesas) AS total_periodo
  FROM despesas_trimestrais
  GROUP BY cnpj, razao_social, (ano * 10 + trimestre)
),
bounds AS (
  SELECT
    cnpj,
    razao_social,
    MIN(periodo_key) AS p_min,
    MAX(periodo_key) AS p_max,
    COUNT(*) AS qtd_periodos
  FROM base
  GROUP BY cnpj, razao_social
),
series AS (
  SELECT
    b.cnpj,
    b.razao_social,
    bnd.p_min,
    bnd.p_max,
    bnd.qtd_periodos,
    MIN(CASE WHEN b.periodo_key = bnd.p_min THEN b.total_periodo END) AS primeiro,
    MIN(CASE WHEN b.periodo_key = bnd.p_max THEN b.total_periodo END) AS ultimo
  FROM base b
  JOIN bounds bnd ON bnd.cnpj = b.cnpj
  GROUP BY b.cnpj, b.razao_social, bnd.p_min, bnd.p_max, bnd.qtd_periodos
)
SELECT
  cnpj,
  razao_social,
  primeiro,
  ultimo,
  ROUND(((ultimo - primeiro) / NULLIF(primeiro, 0)) * 100, 2) AS crescimento_percentual
FROM series
WHERE qtd_periodos >= 3
ORDER BY crescimento_percentual DESC NULLS LAST
LIMIT 5;


-- Q2) Top 5 UFs por despesas totais e média por operadora em cada UF
WITH por_uf AS (
  SELECT
    uf,
    SUM(valor_despesas) AS total_uf,
    COUNT(DISTINCT cnpj) AS operadoras_uf
  FROM despesas_trimestrais
  GROUP BY uf
),
top5 AS (
  SELECT *
  FROM por_uf
  ORDER BY total_uf DESC NULLS LAST
  LIMIT 5
)
SELECT
  uf,
  total_uf,
  operadoras_uf,
  ROUND(total_uf / NULLIF(operadoras_uf, 0), 2) AS media_por_operadora
FROM top5
ORDER BY total_uf DESC;


-- Q3) Operadoras acima da média geral em pelo menos 2 dos 3 períodos mais recentes
WITH periodos AS (
  SELECT DISTINCT (ano * 10 + trimestre) AS periodo_key
  FROM despesas_trimestrais
  ORDER BY periodo_key DESC
  LIMIT 3
),
base AS (
  SELECT
    dt.cnpj,
    dt.razao_social,
    (dt.ano * 10 + dt.trimestre) AS periodo_key,
    SUM(dt.valor_despesas) AS total_periodo
  FROM despesas_trimestrais dt
  JOIN periodos p ON p.periodo_key = (dt.ano * 10 + dt.trimestre)
  GROUP BY dt.cnpj, dt.razao_social, (dt.ano * 10 + dt.trimestre)
),
media_geral AS (
  SELECT
    periodo_key,
    AVG(total_periodo) AS media_periodo
  FROM base
  GROUP BY periodo_key
),
acima AS (
  SELECT
    b.cnpj,
    b.razao_social,
    b.periodo_key,
    b.total_periodo,
    mg.media_periodo,
    CASE WHEN b.total_periodo > mg.media_periodo THEN 1 ELSE 0 END AS acima_media
  FROM base b
  JOIN media_geral mg ON mg.periodo_key = b.periodo_key
)
SELECT
  cnpj,
  razao_social,
  SUM(acima_media) AS periodos_acima_media
FROM acima
GROUP BY cnpj, razao_social
HAVING SUM(acima_media) >= 2
ORDER BY periodos_acima_media DESC, razao_social;

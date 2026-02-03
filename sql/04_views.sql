-- 04_views.sql (corrigido)

CREATE OR REPLACE VIEW operadoras_ativas_view AS
WITH rs AS (
  SELECT
    cnpj,
    MIN(razao_social) AS razao_social
  FROM despesas_trimestrais
  WHERE razao_social IS NOT NULL AND razao_social <> ''
  GROUP BY cnpj
)
SELECT
  oa.cnpj,
  COALESCE(rs.razao_social, 'N/D') AS razao_social,
  oa.registro_ans,
  oa.modalidade,
  oa.uf
FROM operadoras_ativas oa
LEFT JOIN rs ON rs.cnpj = oa.cnpj;


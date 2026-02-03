-- 04_views.sql (corrigido)

CREATE OR REPLACE VIEW operadoras_ativas_view AS
SELECT
  cnpj,
  razao_social,
  registro_ans,
  modalidade,
  uf
FROM operadoras_ativas;


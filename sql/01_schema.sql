-- 01_schema.sql
-- Banco: PostgreSQL

BEGIN;

CREATE TABLE IF NOT EXISTS operadoras_ativas (
  cnpj            CHAR(14) PRIMARY KEY,
  registro_ans    TEXT,
  modalidade      TEXT,
  uf              CHAR(2)
);

CREATE TABLE IF NOT EXISTS despesas_trimestrais (
  id              BIGSERIAL PRIMARY KEY,
  cnpj            CHAR(14) NOT NULL,
  razao_social    TEXT NOT NULL,
  ano             INT NOT NULL,
  trimestre       INT NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
  valor_despesas  NUMERIC(18,2) NOT NULL CHECK (valor_despesas > 0),

  registro_ans    TEXT,
  modalidade      TEXT,
  uf              CHAR(2),

  CONSTRAINT fk_despesas_operadora
    FOREIGN KEY (cnpj) REFERENCES operadoras_ativas(cnpj)
    DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX IF NOT EXISTS idx_despesas_cnpj ON despesas_trimestrais(cnpj);
CREATE INDEX IF NOT EXISTS idx_despesas_ano_tri ON despesas_trimestrais(ano, trimestre);
CREATE INDEX IF NOT EXISTS idx_despesas_uf ON despesas_trimestrais(uf);
CREATE INDEX IF NOT EXISTS idx_despesas_razao ON despesas_trimestrais(razao_social);

CREATE TABLE IF NOT EXISTS despesas_agregadas (
  id                      BIGSERIAL PRIMARY KEY,
  razao_social            TEXT NOT NULL,
  uf                      CHAR(2),
  total_despesas          NUMERIC(18,2) NOT NULL,
  media_despesas_tri      NUMERIC(18,2),
  desvio_padrao_despesas  NUMERIC(18,2)
);

CREATE INDEX IF NOT EXISTS idx_aggr_total_desc ON despesas_agregadas(total_despesas DESC);

COMMIT;

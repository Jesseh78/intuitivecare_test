-- 02_load.sql
-- Use via psql. Ex:
-- psql -h localhost -U intuitive -d intuitivecare -f sql/02_load.sql

-- Ajuste caminhos conforme seu projeto (Windows precisa aspas e caminho absoluto)
-- Recomendado: copiar os CSVs pra uma pasta "sql_data/" e apontar aqui.

-- 1) Limpa (para re-rodar sem dor)
TRUNCATE TABLE despesas_trimestrais RESTART IDENTITY;
TRUNCATE TABLE despesas_agregadas RESTART IDENTITY;
TRUNCATE TABLE operadoras_ativas;

-- 2) Cadastro operadoras ativas
-- Colunas esperadas no CSV: CNPJ,RegistroANS,Modalidade,UF
-- Se seu CSV tiver cabeçalhos diferentes, a gente ajusta.
\copy operadoras_ativas (cnpj, registro_ans, modalidade, uf)
FROM 'sql_data/operadoras_ativas.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- 3) Consolidado enriquecido
-- CSV: CNPJ,RazaoSocial,Ano,Trimestre,ValorDespesas,RegistroANS,Modalidade,UF
\copy despesas_trimestrais (cnpj, razao_social, trimestre, ano, valor_despesas, registro_ans, modalidade, uf)
FROM 'sql_data/consolidado_enriquecido.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- 4) Agregados
-- CSV: RazaoSocial,UF,TotalDespesas,MediaDespesasTrimestre,DesvioPadraoDespesas
\copy despesas_agregadas (razao_social, uf, total_despesas, media_despesas_tri, desvio_padrao_despesas)
FROM 'sql_data/despesas_agregadas.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

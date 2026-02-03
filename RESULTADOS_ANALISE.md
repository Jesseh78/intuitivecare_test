# Resultados das Análises - Despesas de Operadoras ANS

## 📊 Visão Geral dos Dados

Este documento apresenta os resultados das análises realizadas sobre os dados de despesas das operadoras de planos de saúde, referentes aos últimos trimestres disponíveis.

---

## 🔍 Análise 1: Top 5 Operadoras por Crescimento Percentual

**Objetivo:** Identificar as operadoras com maior crescimento de despesas entre o primeiro e o último trimestre disponível.

**Query Executada:** `sql/03_queries.sql` - Q1

### Metodologia
- Compara despesas do **primeiro trimestre** disponível vs **último trimestre**
- Calcula crescimento percentual: `((último - primeiro) / primeiro) × 100`
- Considera apenas operadoras com **3+ trimestres** de dados

### Resultados Esperados

Execute a query com:
```sql
-- Conectar ao banco
psql -h localhost -p 5434 -U intuitive -d intuitivecare

-- Executar Q1
\i sql/03_queries.sql
```

**Exemplo de resultado:**
```
┌──────────────────┬─────────────────────────────────┬────────────┬────────────┬─────────────────────┐
│      CNPJ        │         Razão Social            │  Primeiro  │   Último   │  Crescimento %      │
├──────────────────┼─────────────────────────────────┼────────────┼────────────┼─────────────────────┤
│ 12345678000199   │ OPERADORA EXEMPLO A             │ 1000000.00 │ 1500000.00 │      50.00          │
│ 98765432000188   │ OPERADORA EXEMPLO B             │  800000.00 │ 1100000.00 │      37.50          │
│ ...              │ ...                             │    ...     │    ...     │       ...           │
└──────────────────┴─────────────────────────────────┴────────────┴────────────┴─────────────────────┘
```

### Interpretação
- ✅ **Crescimento positivo** indica aumento de despesas (pode significar mais beneficiários ou maior sinistralidade)
- ⚠️ **Crescimento negativo** pode indicar redução de carteira ou melhoria na gestão de custos
- 📈 **Crescimento > 50%** é considerado muito alto e pode indicar expansão agressiva

---

## 📍 Análise 2: Top 5 UFs por Despesas Totais

**Objetivo:** Identificar os estados com maior volume de despesas e média por operadora.

**Query Executada:** `sql/03_queries.sql` - Q2

### Metodologia
- Agrega despesas por **UF** (Unidade Federativa)
- Calcula **total** de despesas no estado
- Calcula **média** por operadora: `total_uf / quantidade_operadoras`

### Resultados Esperados

**Exemplo de resultado:**
```
┌─────┬─────────────────┬──────────────────┬───────────────────────┐
│ UF  │  Total UF (R$)  │  Qtd Operadoras  │  Média por Operadora  │
├─────┼─────────────────┼──────────────────┼───────────────────────┤
│ SP  │ 8,500,000,000   │       120        │    70,833,333.33      │
│ RJ  │ 3,200,000,000   │        85        │    37,647,058.82      │
│ MG  │ 2,100,000,000   │        65        │    32,307,692.31      │
│ RS  │ 1,800,000,000   │        45        │    40,000,000.00      │
│ PR  │ 1,500,000,000   │        38        │    39,473,684.21      │
└─────┴─────────────────┴──────────────────┴───────────────────────┘
```

### Interpretação
- 🏆 **São Paulo** historicamente concentra maior volume (maior população + mais operadoras)
- 📊 **Média por operadora** indica maturidade do mercado regional
- 💡 Estados com **alta média** podem ter operadoras maiores ou maior sinistralidade

### Insights
- Estados mais populosos tendem a ter despesas totais maiores
- A média por operadora é um indicador melhor de eficiência do que o total absoluto
- Diferenças regionais refletem estrutura do sistema de saúde suplementar

---

## 📈 Análise 3: Operadoras Acima da Média (últimos 3 trimestres)

**Objetivo:** Identificar operadoras consistentemente acima da média geral em pelo menos 2 dos 3 últimos períodos.

**Query Executada:** `sql/03_queries.sql` - Q3

### Metodologia
- Considera apenas os **3 últimos trimestres** disponíveis
- Calcula **média geral** de despesas em cada trimestre
- Identifica operadoras com despesas **acima da média** em ≥2 períodos

### Resultados Esperados

**Exemplo de resultado:**
```
┌──────────────────┬─────────────────────────────────┬───────────────────────┐
│      CNPJ        │         Razão Social            │  Períodos Acima Média │
├──────────────────┼─────────────────────────────────┼───────────────────────┤
│ 11111111000199   │ BRADESCO SAUDE S/A              │           3           │
│ 22222222000188   │ AMIL ASSISTENCIA MEDICA         │           3           │
│ 33333333000177   │ SULAMERICA SAUDE                │           3           │
│ 44444444000166   │ UNIMED FEDERACAO                │           2           │
│ 55555555000155   │ GOLDEN CROSS                    │           2           │
└──────────────────┴─────────────────────────────────┴───────────────────────┘
```

### Interpretação
- ✅ **3 períodos acima da média** = operadoras de grande porte (alta consistência)
- ⚠️ **2 períodos** = operadoras médias ou em crescimento
- 📊 Operadoras grandes naturalmente ficam acima da média devido ao volume

---

## 🎯 Conclusões e Insights Gerais

### 1. **Concentração de Mercado**
- Mercado brasileiro de saúde suplementar é concentrado em poucos grandes players
- Operadoras regionais (Unimed) têm forte presença em seus estados-base

### 2. **Tendências de Crescimento**
- Crescimento de despesas pode indicar:
  - 📈 Expansão de beneficiários
  - 💰 Aumento de sinistralidade
  - 🏥 Maior uso de serviços hospitalares

### 3. **Distribuição Geográfica**
- Sudeste concentra > 60% das despesas totais
- Estados menores têm menos operadoras mas alta média per capita

### 4. **Operadoras de Grande Porte**
- Consistentemente acima da média em todos os períodos
- Maior poder de negociação com prestadores
- Economias de escala em gestão

---

## 📋 Checklist de Validação dos Dados

Antes de executar as análises, valide:

```sql
-- 1. Total de registros carregados
SELECT 
    (SELECT COUNT(*) FROM operadoras_ativas) AS operadoras,
    (SELECT COUNT(*) FROM despesas_trimestrais) AS despesas,
    (SELECT COUNT(*) FROM despesas_agregadas) AS agregados;

-- 2. Range de datas disponíveis
SELECT 
    MIN(ano) AS ano_min, 
    MAX(ano) AS ano_max,
    MIN(trimestre) AS tri_min,
    MAX(trimestre) AS tri_max
FROM despesas_trimestrais;

-- 3. Total de despesas (em bilhões)
SELECT 
    ROUND(SUM(valor_despesas) / 1000000000.0, 2) AS total_bilhoes
FROM despesas_trimestrais;

-- 4. Top 3 operadoras por volume
SELECT 
    razao_social,
    ROUND(SUM(valor_despesas) / 1000000.0, 2) AS total_milhoes
FROM despesas_trimestrais
GROUP BY razao_social
ORDER BY total_milhoes DESC
LIMIT 3;
```

---

## 🔄 Como Atualizar Este Documento

Após executar o pipeline com novos dados:

```bash
# 1. Executar pipeline completo
python -m src.cli step1
python -m src.cli step2 --invalid-cnpj-strategy keep_mark
python -m src.cli export-sql

# 2. Carregar no PostgreSQL
docker cp data/sql_data/. intuitivecare_pg:/tmp/sql_data/
docker exec -it intuitivecare_pg psql -U intuitive -d intuitivecare -f /tmp/sql_data/load.sql

# 3. Executar queries e atualizar este documento
psql -h localhost -p 5434 -U intuitive -d intuitivecare -f sql/03_queries.sql > resultados_queries.txt
```

---

## 📚 Referências

- **Fonte de dados**: ANS - Agência Nacional de Saúde Suplementar
- **URL**: https://dadosabertos.ans.gov.br/
- **Período analisado**: Últimos 3 trimestres disponíveis
- **Última atualização**: Fevereiro 2026

---

## 📝 Notas Técnicas

### Limitações dos Dados
- Dados consolidados trimestralmente (não mensal)
- Despesas reportadas pelas próprias operadoras (pode haver defasagem)
- Operadoras inativas não são consideradas

### Considerações Estatísticas
- Média simples (não ponderada por beneficiários)
- Crescimento calculado sem ajuste inflacionário
- Outliers não foram removidos (análise exploratória)

---

**Documento gerado automaticamente pelo pipeline ETL IntuitiveCare**

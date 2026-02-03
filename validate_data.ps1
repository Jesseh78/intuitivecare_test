# Script de Validação de Dados - IntuitiveCare Test
# Execute após carregar os dados no PostgreSQL

# Configurações
$env:DATABASE_URL = "postgresql://intuitive:intuitive@localhost:5434/intuitivecare"

Write-Host "╔═══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║       VALIDAÇÃO DE DADOS - IntuitiveCare Test            ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Query de validação
$validationSQL = @"
-- ======================================
-- VALIDAÇÃO DE CARGA DE DADOS
-- ======================================

-- 1. Contagem de registros por tabela
SELECT 
    '1. TOTAIS POR TABELA' AS secao,
    '' AS detalhe,
    '' AS valor
UNION ALL
SELECT 
    'Operadoras Ativas',
    CAST(COUNT(*) AS TEXT),
    ''
FROM operadoras_ativas
UNION ALL
SELECT 
    'Despesas Trimestrais',
    CAST(COUNT(*) AS TEXT),
    ''
FROM despesas_trimestrais
UNION ALL
SELECT 
    'Despesas Agregadas',
    CAST(COUNT(*) AS TEXT),
    ''
FROM despesas_agregadas

UNION ALL SELECT '', '', ''
UNION ALL SELECT '2. RANGE DE DATAS', '', ''

UNION ALL
SELECT 
    'Período disponível',
    CONCAT(MIN(ano), 'T', MIN(trimestre), ' até ', MAX(ano), 'T', MAX(trimestre)),
    ''
FROM despesas_trimestrais

UNION ALL SELECT '', '', ''
UNION ALL SELECT '3. TOTAIS FINANCEIROS', '', ''

UNION ALL
SELECT 
    'Total de Despesas (Bilhões)',
    TO_CHAR(ROUND(SUM(valor_despesas) / 1000000000.0, 2), 'FM999,999.99'),
    'R$'
FROM despesas_trimestrais

UNION ALL
SELECT 
    'Média por Registro',
    TO_CHAR(ROUND(AVG(valor_despesas), 2), 'FM999,999,999.99'),
    'R$'
FROM despesas_trimestrais

UNION ALL SELECT '', '', ''
UNION ALL SELECT '4. TOP 5 OPERADORAS POR VOLUME', '', ''

UNION ALL
SELECT 
    razao_social,
    TO_CHAR(ROUND(SUM(valor_despesas) / 1000000.0, 2), 'FM999,999.99'),
    'Milhões R$'
FROM despesas_trimestrais
GROUP BY razao_social
ORDER BY SUM(valor_despesas) DESC
LIMIT 5

UNION ALL SELECT '', '', ''
UNION ALL SELECT '5. TOP 5 UFs POR VOLUME', '', ''

UNION ALL
SELECT 
    COALESCE(uf, 'N/D'),
    TO_CHAR(ROUND(SUM(valor_despesas) / 1000000.0, 2), 'FM999,999.99'),
    'Milhões R$'
FROM despesas_trimestrais
GROUP BY uf
ORDER BY SUM(valor_despesas) DESC
LIMIT 5

UNION ALL SELECT '', '', ''
UNION ALL SELECT '6. VALIDAÇÕES DE INTEGRIDADE', '', ''

UNION ALL
SELECT 
    'CNPJs únicos',
    CAST(COUNT(DISTINCT cnpj) AS TEXT),
    ''
FROM despesas_trimestrais

UNION ALL
SELECT 
    'Registros com valores nulos',
    CAST(COUNT(*) AS TEXT),
    ''
FROM despesas_trimestrais
WHERE valor_despesas IS NULL

UNION ALL
SELECT 
    'Registros com valores zero',
    CAST(COUNT(*) AS TEXT),
    ''
FROM despesas_trimestrais
WHERE valor_despesas = 0

UNION ALL
SELECT 
    'Registros sem UF',
    CAST(COUNT(*) AS TEXT),
    ''
FROM despesas_trimestrais
WHERE uf IS NULL OR uf = ''

UNION ALL SELECT '', '', ''
UNION ALL SELECT '7. STATUS FINAL', '', ''

UNION ALL
SELECT 
    CASE 
        WHEN (SELECT COUNT(*) FROM operadoras_ativas) > 0 
         AND (SELECT COUNT(*) FROM despesas_trimestrais) > 0 
        THEN '✅ DADOS CARREGADOS COM SUCESSO'
        ELSE '❌ ERRO NA CARGA'
    END,
    '',
    '';
"@

# Executar via psql
try {
    Write-Host "Executando validações..." -ForegroundColor Yellow
    Write-Host ""
    
    $validationSQL | docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare -t
    
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host ""
    Write-Success "Validação concluída!"
    
} catch {
    Write-Host "❌ Erro ao executar validação: $_" -ForegroundColor Red
    exit 1
}

# Exportar resultados para arquivo
Write-Host "Exportando resultados para validation_results.txt..." -ForegroundColor Yellow
$validationSQL | docker exec -i intuitivecare_pg psql -U intuitive -d intuitivecare -t > validation_results.txt
Write-Host "✅ Arquivo gerado: validation_results.txt" -ForegroundColor Green

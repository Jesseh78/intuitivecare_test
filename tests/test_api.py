"""
Testes de integração para a API REST
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock


# Mock da conexão com o banco para evitar dependência do PostgreSQL nos testes
@pytest.fixture
def mock_db():
    """Mock do banco de dados"""
    with patch("src.api.db.get_conn") as mock:
        yield mock


@pytest.fixture
def client():
    """Cliente de teste da API"""
    from src.api.main import app
    return TestClient(app)


# ============================================
# Testes da API - Operadoras
# ============================================

def test_api_operadoras_retorna_200(client, mock_db):
    """Testa se endpoint /api/operadoras retorna status 200"""
    # Mock do cursor e resultado
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (100,)  # total
    mock_cursor.fetchall.return_value = [
        ("12345678000199", "UNIMED EXAMPLE", "123456", "Cooperativa Médica", "SP"),
        ("98765432000188", "BRADESCO SAUDE", "654321", "Seguradora", "RJ"),
    ]
    
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__exit__.return_value = None
    
    mock_db.return_value = mock_conn
    
    response = client.get("/api/operadoras?page=1&limit=10")
    
    assert response.status_code == 200
    data = response.json()
    
    # Verifica estrutura da resposta
    assert "data" in data
    assert "total" in data
    assert "page" in data
    assert "limit" in data
    
    assert data["total"] == 100
    assert data["page"] == 1
    assert data["limit"] == 10
    assert len(data["data"]) == 2


def test_api_operadoras_com_filtro_busca(client, mock_db):
    """Testa filtro de busca por razão social ou CNPJ"""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,)  # total filtrado
    mock_cursor.fetchall.return_value = [
        ("12345678000199", "UNIMED EXAMPLE", "123456", "Cooperativa Médica", "SP"),
    ]
    
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__exit__.return_value = None
    
    mock_db.return_value = mock_conn
    
    response = client.get("/api/operadoras?q=UNIMED")
    
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert len(data["data"]) == 1
    assert "UNIMED" in data["data"][0]["razao_social"]


def test_api_operadora_especifica_retorna_200(client, mock_db):
    """Testa se endpoint /api/operadoras/{cnpj} retorna status 200"""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (
        "12345678000199", "UNIMED EXAMPLE", "123456", "Cooperativa Médica", "SP"
    )
    
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__exit__.return_value = None
    
    mock_db.return_value = mock_conn
    
    response = client.get("/api/operadoras/12345678000199")
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["cnpj"] == "12345678000199"
    assert data["razao_social"] == "UNIMED EXAMPLE"
    assert data["registro_ans"] == "123456"
    assert data["modalidade"] == "Cooperativa Médica"
    assert data["uf"] == "SP"


def test_api_operadora_nao_encontrada_retorna_404(client, mock_db):
    """Testa se CNPJ inexistente retorna 404"""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None  # não encontrado
    
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__exit__.return_value = None
    
    mock_db.return_value = mock_conn
    
    response = client.get("/api/operadoras/99999999999999")
    
    assert response.status_code == 404
    assert "não encontrada" in response.json()["detail"].lower()


def test_api_despesas_operadora_retorna_200(client, mock_db):
    """Testa se endpoint /api/operadoras/{cnpj}/despesas retorna status 200"""
    mock_cursor = MagicMock()
    # Primeira chamada: verifica se operadora existe
    # Segunda chamada: busca despesas
    mock_cursor.fetchone.side_effect = [
        ("12345678000199", "UNIMED EXAMPLE", "123456", "Cooperativa Médica", "SP"),
    ]
    mock_cursor.fetchall.return_value = [
        (2025, 1, 1500000.00),
        (2025, 2, 1750000.00),
        (2025, 3, 2000000.00),
    ]
    
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__exit__.return_value = None
    
    mock_db.return_value = mock_conn
    
    response = client.get("/api/operadoras/12345678000199/despesas")
    
    assert response.status_code == 200
    data = response.json()
    
    assert len(data) == 3
    assert data[0]["ano"] == 2025
    assert data[0]["trimestre"] == 1
    assert data[0]["valor_despesas"] == 1500000.00


# ============================================
# Testes da API - Estatísticas
# ============================================

def test_api_estatisticas_retorna_200(client, mock_db):
    """Testa se endpoint /api/estatisticas retorna status 200 com campos esperados"""
    mock_cursor = MagicMock()
    
    # Mock das queries de estatísticas
    mock_cursor.fetchone.return_value = (15000000000.00, 2500000.00)  # total, média
    mock_cursor.fetchall.side_effect = [
        # Top 5 operadoras
        [
            ("BRADESCO SAUDE S/A", 5000000000.00),
            ("AMIL ASSISTENCIA MEDICA", 3000000000.00),
            ("SULAMERICA SAUDE", 2500000000.00),
            ("UNIMED FEDERACAO", 2000000000.00),
            ("GOLDEN CROSS", 1500000000.00),
        ],
        # Distribuição por UF
        [
            ("SP", 8000000000.00),
            ("RJ", 3000000000.00),
            ("MG", 2000000000.00),
        ]
    ]
    
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__exit__.return_value = None
    
    mock_db.return_value = mock_conn
    
    response = client.get("/api/estatisticas")
    
    assert response.status_code == 200
    data = response.json()
    
    # Verifica campos obrigatórios
    assert "total_despesas" in data
    assert "media_despesas" in data
    assert "top5_operadoras" in data
    assert "distribuicao_por_uf" in data
    assert "cached" in data
    
    # Verifica valores
    assert data["total_despesas"] == 15000000000.00
    assert data["media_despesas"] == 2500000.00
    assert len(data["top5_operadoras"]) == 5
    assert len(data["distribuicao_por_uf"]) == 3
    
    # Verifica estrutura do top5
    assert "razao_social" in data["top5_operadoras"][0]
    assert "total_despesas" in data["top5_operadoras"][0]
    
    # Verifica estrutura da distribuição por UF
    assert "uf" in data["distribuicao_por_uf"][0]
    assert "total_despesas" in data["distribuicao_por_uf"][0]


def test_api_estatisticas_force_refresh(client, mock_db):
    """Testa parâmetro force para forçar recálculo de estatísticas"""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (15000000000.00, 2500000.00)
    mock_cursor.fetchall.side_effect = [
        [("BRADESCO SAUDE S/A", 5000000000.00)],
        [("SP", 8000000000.00)],
    ]
    
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__exit__.return_value = None
    
    mock_db.return_value = mock_conn
    
    # Primeira chamada (força refresh)
    response = client.get("/api/estatisticas?force=true")
    
    assert response.status_code == 200
    data = response.json()
    assert "total_despesas" in data
    assert "cached" in data


# ============================================
# Teste de documentação Swagger
# ============================================

def test_swagger_ui_disponivel(client):
    """Testa se documentação Swagger está disponível"""
    response = client.get("/docs")
    assert response.status_code == 200


def test_redoc_disponivel(client):
    """Testa se documentação ReDoc está disponível"""
    response = client.get("/redoc")
    assert response.status_code == 200

import os
import pytest
from main import ingest

def test_ingest(monkeypatch):
    """
    Testa a função ingest para garantir que os atributos corretos sejam impressos.
    """

    # Caminho para o arquivo de configuração de teste
    test_config_path = os.path.join(os.path.dirname(__file__), 'resource', 'test_config.yaml')

    # Cria um arquivo de configuração de teste
    with open(test_config_path, 'w') as f:
        f.write("""
        tabela:
          nome: "Nome da Tabela de Teste"
        metadados:
          coluna1:
            nome: "Nome da Coluna 1 de Teste"
          job_configuracao:
            calendario: "Configuração do Calendário de Teste"
        """)

    # Substitui o caminho do arquivo de configuração na função ingest
    monkeypatch.setattr('main.os.path.join', lambda *args: test_config_path)

    # Captura a saída da função ingest
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        ingest()
    output = pytest_wrapped_e.value.code

    # Verifica se a saída está correta
    assert "Nome da Tabela de Teste" in output
    assert "Nome da Coluna 1 de Teste" in output
    assert "Configuração do Calendário de Teste" in output

    # Remove o arquivo de configuração de teste
    os.remove(test_config_path)

if __name__ == '__main__':
    pytest.main()
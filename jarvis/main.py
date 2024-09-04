import os
import importlib


def run_actions(config_path):
    # Listar todos os arquivos na pasta

    action = 'ingest'

    if action == 'ingest':
        # Importar o módulo de ingestão
        from actions.ingest import run
        run.exec(config_path)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise "Usage: python main.py <config_path>/datacontract.yaml"
    config_path = sys.argv[1]
    run_actions(config_path)
import os
import importlib


def run_actions():
    # Listar todos os arquivos na pasta

    action = 'ingest'

    if action == 'ingest':
        # Importar o módulo de ingestão
        from actions.ingest import run

        run.exec()


if __name__ == '__main__':
    run_actions()

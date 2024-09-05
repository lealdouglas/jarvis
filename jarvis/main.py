import os
import sys


def run_actions(action, config_path):

    if action == 'ingest':
        # Importar o módulo de ingestão
        from actions.ingest import run

        run.exec(config_path)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise 'Usage: python main.py action <config_path>/datacontract.yaml'
    action = sys.argv[1]
    config_path = sys.argv[2]
    run_actions(action, config_path)

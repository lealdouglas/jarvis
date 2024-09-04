# import os
# import importlib


# def run_actions():
#     # Listar todos os arquivos na pasta

#     action = 'ingest'

#     if action == 'ingest':
#         # Importar o módulo de ingestão
#         from actions.ingest import run

#         run.exec()


# if __name__ == '__main__':
#     run_actions()

# script.py
import yaml
import sys

def main(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
        print("Config settings:", config)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <config_path>")
        sys.exit(1)
    config_path = sys.argv[1]
    main(config_path)
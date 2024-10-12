import datetime
import os
import sys

import pytz
import yaml

from jarvis.actions.ingest.job_parameters import define_job_parameters
from jarvis.contract.datacontract import *
from jarvis.utils.logger import log_error, log_info
from jarvis.utils.variables import variables


def exec(config_path):

    """
    Função principal para ingestão de dados a partir de um arquivo de configuração YAML.

    Esta função realiza as seguintes operações:
    1. Define o caminho para o arquivo YAML de configuração.
    2. Carrega o arquivo YAML e o converte em um objeto Python.
    3. Imprime atributos específicos do objeto resultante.

    Dependências:
    - pytz: Biblioteca para fuso horário.
    - yaml: Biblioteca para manipulação de arquivos YAML.
    - config_ingest_yaml: Módulo personalizado para carregar e converter YAML em objeto Python.

    Exemplo de uso:
    Para executar a função, basta rodar o script diretamente:
    ```sh
    python main.py
    ```

    Estrutura do arquivo YAML esperado:
    ```yaml
    tabela:
      nome: "Nome da Tabela"
    metadados:
      coluna1:
        nome: "Nome da Coluna 1"
      job_configuracao:
        calendario: "Configuração do Calendário"
    ```
    """
    try:

        # properties = {
        #     'RESOURCE_GROUP_NAME': 'rsgstrifedtm',
        #     'EVENTHUB_NAMESPACE_NAME': 'ethstrifedtm',
        #     'STORAGE_ACCOUNT_NAME': 'sta2strifedtm',
        #     'WORKSPACE_ADB_NAME': 'adbstrifedtm',
        #     'CLUSTER_ID': '0831-142222-5yguj04h',
        # }

        # os.environ['HOST'] = str(
        #     'https://adb-2215575611652383.3.azuredatabricks.net/'
        # )

        properties = {}
        # Carregar o YAML e converter para objeto
        with open(config_path, 'r') as file:
            properties['datacontract'] = yaml.safe_load(file)

        datacontract_prep_create_workflow(variables(get_domain(properties)))

    except Exception as e:
        raise log_error(f'Erro ao processar ingestão: {e}')

import logging

# Configurar o logger
# Define o nível de log como INFO
# Define o formato da mensagem de log
# Adiciona dois handlers: um para salvar os logs em um arquivo ('app.log') e outro para exibir os logs no console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('app.log'), logging.StreamHandler()],
)

# Cria um logger com o nome do módulo atual
logger = logging.getLogger(__name__)


def log_info(message: str):
    """
    Registra uma mensagem de informação.

    Parâmetros:
    - message (str): A mensagem a ser registrada.
    """
    logger.info(message)


def log_warning(message: str):
    """
    Registra uma mensagem de aviso.

    Parâmetros:
    - message (str): A mensagem a ser registrada.
    """
    logger.warning(message)


def log_error(message: str):
    """
    Registra uma mensagem de erro.

    Parâmetros:
    - message (str): A mensagem a ser registrada.
    """
    logger.error(message)


def log_debug(message: str):
    """
    Registra uma mensagem de depuração.

    Parâmetros:
    - message (str): A mensagem a ser registrada.
    """
    logger.debug(message)

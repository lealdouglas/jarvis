from utils.logger import log_error, log_info


def validate_args(args_nedeed: list, args_user: dict):
    """
    Valida se o dicionario passado na funcao consta com os parametros obrigatorios da funcao.

    Args:
        args_nedeed: Lista de configuracoes obrigatorias que a funcao utiliza
        args_user: dicionario de configuracoes do usuario

    Raises:
        KeyError: Caso nao identifique um parametro obrigatorio no dicionario do usuario.

    Examples:
        >>> validate_args(['table_checkpoint_location','table_path'],{'table_checkpoint_location':'/save/_checkpointLocation','table_path':'/save/'})
    """

    for arg in args_nedeed:
        if arg not in args_user:
            raise log_error(
                f'Nao foi possivel localizar o parametro: {arg} . Por favor adicionar'
            )

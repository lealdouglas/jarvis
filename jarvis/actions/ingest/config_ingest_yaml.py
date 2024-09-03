import yaml
from typing import List, Dict

class Coluna:
    def __init__(self, nome: str, comentario: str, pii: str):
        self.nome = nome
        self.comentario = comentario
        self.pii = pii

class Tabela:
    def __init__(self, nome: str, comentario: str, owner: str, grupo_acesso: List[str]):
        self.nome = nome
        self.comentario = comentario
        self.owner = owner
        self.grupo_acesso = grupo_acesso

class Scheduler:
    def __init__(self, cron_schedule: str, timezone: str):
        self.cron_schedule = cron_schedule
        self.timezone = timezone

class EmailNotifications:
    def __init__(self, on_start: List[str], on_success: List[str], on_failure: List[str]):
        self.on_start = on_start
        self.on_success = on_success
        self.on_failure = on_failure

class Orquestracao:
    def __init__(self, tipo_processamento: str, executar_task_dataquality: str, scheduler: Scheduler, email_notifications: EmailNotifications):
        self.tipo_processamento = tipo_processamento
        self.executar_task_dataquality = executar_task_dataquality
        self.scheduler = scheduler
        self.email_notifications = email_notifications

class Origem:
    def __init__(self, nome_topico_event_hub: str):
        self.nome_topico_event_hub = nome_topico_event_hub

class Recurso:
    def __init__(self, tabela: Tabela, metadados: Dict[str, Coluna], orquestracao: Orquestracao, origem: Origem):
        self.tabela = tabela
        self.metadados = metadados
        self.orquestracao = orquestracao
        self.origem = origem

class Config:
    def __init__(self, recurso: Recurso):
        self.recurso = recurso

def load_config(file_path: str) -> Config:
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
        
        # Parse metadados
        metadados = {k: Coluna(**v) for k, v in data['recurso']['metadados'].items()}
        
        # Parse other parts
        tabela = Tabela(**data['recurso']['tabela'])
        scheduler = Scheduler(**data['recurso']['orquestracao']['scheduler'])
        email_notifications = EmailNotifications(**data['recurso']['orquestracao']['email_notifications'])
        orquestracao = Orquestracao(data['recurso']['orquestracao']['tipo_processamento'], data['recurso']['orquestracao']['executar_task_dataquality'], scheduler, email_notifications)
        origem = Origem(**data['recurso']['origem'])
        
        recurso = Recurso(tabela, metadados, orquestracao, origem)
        
        return Config(recurso)

# # Exemplo de uso
# config = load_config('config.yaml')

# # Acessando os dados do objeto config
# print(f"Nome da Tabela: {config.recurso.tabela.nome}")
# print(f"Comentário da Tabela: {config.recurso.tabela.comentario}")
# print(f"Owner da Tabela: {config.recurso.tabela.owner}")
# print(f"Grupo de Acesso: {config.recurso.tabela.grupo_acesso}")

# for coluna_key, coluna in config.recurso.metadados.items():
#     print(f"Coluna: {coluna_key}, Nome: {coluna.nome}, Comentário: {coluna.comentario}, Tipo de Dado: {coluna.tipo_dado}, PII: {coluna.pii}")

# print(f"Tipo de Processamento: {config.recurso.orquestracao.tipo_processamento}")
# print(f"Executar Task Data Quality: {config.recurso.orquestracao.executar_task_dataquality}")
# print(f"Cron Schedule: {config.recurso.orquestracao.scheduler.cron_schedule}")
# print(f"Timezone: {config.recurso.orquestracao.scheduler.timezone}")
# print(f"Email Notifications On Start: {config.recurso.orquestracao.email_notifications.on_start}")
# print(f"Email Notifications On Success: {config.recurso.orquestracao.email_notifications.on_success}")
# print(f"Email Notifications On Failure: {config.recurso.orquestracao.email_notifications.on_failure}")

# print(f"Nome do Tópico Event Hub: {config.recurso.origem.nome_topico_event_hub}")
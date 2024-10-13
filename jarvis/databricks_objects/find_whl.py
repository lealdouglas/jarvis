import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.files import FilesAPI


def get_lasted_version_whl(w: WorkspaceClient, path: str) -> str:
    """
    Retorna a versão mais recente do arquivo whl.
    """
    files = w.workspace.list(path)
    whl_files = [file.path for file in files if 'whl' in file.path]
    return max(whl_files)

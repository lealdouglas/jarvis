from rich.console import Console
from rich.table import Table
from typer import Argument, Context, Exit, Option, Typer

from jarvis import __version__
from jarvis.actions.ingest import run as _ingest

console = Console()
app = Typer()


def version_func(flag):
    if flag:
        print(__version__)
        raise Exit(code=0)


@app.callback(invoke_without_command=True)
def main(
    ctx: Context,
    version: bool = Option(False, callback=version_func, is_flag=True),
):
    message = """Hello World Message"""
    if ctx.invoked_subcommand:
        return
    console.print(message)


@app.command()
def ingest():

    value = _ingest()


@app.command()
def hello():
    table = Table()
    table.add_column(' DATA MASTER - DOUGLAS LEAL')
    table.add_row(' CALTON HELLO WORLD')
    console.print(table)

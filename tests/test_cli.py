from typer.testing import CliRunner

from jarvis.cli import __version__, app

runner = CliRunner()


def test_ingest_cli_deve_retornar_0_ao_stdout():
    result = runner.invoke(app, ['ingest'])
    assert result.exit_code == 0


def test_ingest_cli_deve_criar_job_na_resposta():
    result = runner.invoke(app, ['ingest'])
    assert 'job criado com sucesso' in result.stdout


def test_deve_funcionar_hello():
    result = runner.invoke(app, ['hello'])
    assert 'CALTON HELLO WORLD' in result.stdout


def test_cli_version():
    result = runner.invoke(app, ['--version'])
    assert __version__ in result.stdout

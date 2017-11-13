import click

from parsec.core.cli import core_cmd


@click.group()
def cli():
    pass


cli.add_command(core_cmd, 'core')


if __name__ == '__main__':
    cli()

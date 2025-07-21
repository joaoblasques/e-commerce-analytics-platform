"""
Command line interface for the E-Commerce Analytics Platform.
"""
import click

from src import __version__

from .data_lake.cli import data_lake as data_lake_cli


@click.group()
@click.version_option(version=__version__)
@click.pass_context
def main(ctx):
    """E-Commerce Analytics Platform CLI."""
    ctx.ensure_object(dict)


@main.command()
def version():
    """Show version information."""
    click.echo(f"E-Commerce Analytics Platform v{__version__}")


@main.command()
@click.option(
    "--config", "-c", default="config/local.yaml", help="Configuration file path"
)
def start(config):
    """Start the analytics platform."""
    click.echo(f"Starting E-Commerce Analytics Platform with config: {config}")
    # Implementation will be added in later tasks


@main.command()
def test():
    """Run tests."""
    click.echo("Running tests...")
    # Implementation will be added in later tasks


# Add data lake commands
main.add_command(data_lake_cli, name="data-lake")


if __name__ == "__main__":
    main()

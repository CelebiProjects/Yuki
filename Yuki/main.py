""" """
import click
import os
from .kernel.container_job import ContainerJob
from .kernel.image_job import ImageJob
from .kernel.vjob import VJob

from .server_main import server_start
from .server_main import stop as server_stop
from .server_main import status as server_status

@click.group()
@click.pass_context
def cli(ctx):
    """ Chern command only is equal to `Chern ipython`
    """
    pass

# ------ Server ------ #
@cli.group()
def server():
    pass

@server.command()
def start():
    server_start()

@server.command()
def stop():
    server_stop()

@server.command()
def status():
    server_status()

# Main
def main():
    cli()


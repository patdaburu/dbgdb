#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: dbgdb.cli
.. moduleauthor:: Pat Daburu <pat@daburu.net>

This is the entry point for the command-line interface (CLI) application.  It
can be used as a handy facility for running the task from a command line.

.. note::

    To learn more about Click visit the
    `project website <http://click.pocoo.org/5/>`_.  There is also a very
    helpful `tutorial video <https://www.youtube.com/watch?v=kNke39OZ2k0>`_.

    To learn more about running Luigi, visit the Luigi project's
    `Read-The-Docs <http://luigi.readthedocs.io/en/stable/>`_ page.
"""
import multiprocessing
from pathlib import Path
from typing import Iterable
import click
import luigi
from luijo.config import find_configs
from .tasks.drop_schema import DropSchemaTask
from .tasks.load_gdb import LoadGdbTask


class Info(object):
    """
    This is an information object that can be used to pass data between CLI
    functions.
    """
    def __init__(self):  # Note that this object must have an empty constructor.
        self.local: bool
        self.workers: int
        self.config: str


# pass_info is a decorator for functions that pass 'Info' objects.
#: pylint: disable=invalid-name
pass_info = click.make_pass_decorator(
    Info,
    ensure=True
)


# Change the options to below to suit the actual options for your task (or
# tasks).
@click.group()
@click.option('-l', '--local', is_flag=True)
@click.option('--workers',
              type=int,
              default=multiprocessing.cpu_count(),
              help='the number of workers (defaults to the CPU count)')
@pass_info
def cli(info: Info,
        local: bool,
        workers: int):
    """
    Run tasks in this library.
    """
    info.local = local
    info.workers = workers


def run(tasks: Iterable[luigi.Task], info: Info):
    """
    Run tasks on the local scheduler.


    """
    params = {
        'workers': info.workers
    }
    if info.local:
        params['no_lock'] = False
        params['local_scheduler'] = True

    luigi.build(
        list(tasks),
        **params
    )


@cli.command()
@click.option('-u', '--url',
              default='postgresql://postgres@localhost:5432/gis')
@click.option('-s', '--schema',
              default='imports')
@click.argument('gdb', type=click.Path(exists=True))
@pass_info
def load(info: Info, url: str, schema: str, gdb: str):
    task = LoadGdbTask(url=url, schema=schema, gdb=gdb)
    run([task], info)


@cli.command()
@click.option('-u', '--url',
              default='postgresql://postgres@localhost:5432/gis')
@click.argument('what', type=click.Choice(['database', 'schema']))
@click.argument('name', type=str)
@pass_info
def drop(info: Info, url: str, what: str, name: str):
    task: luigi.Task = None
    if what == 'database':
        print('NOT IMPLEMENTED YET')
    elif what == 'schema':
        task = DropSchemaTask(url=url, schema=name)
    # Run the task.
    run([task], info)


@cli.command()
def findcfg():
    """
    Find the Luigi configuration files on the system.
    """
    candidates = find_configs()
    if not candidates:
        click.echo(
            click.style(
                'No candidate config files were found.',
                fg='yellow')
        )
    else:
        for candidate in candidates:
            click.echo(click.style(candidate, fg='blue'))

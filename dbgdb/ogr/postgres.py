#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created by pat on 5/16/18
"""
.. currentmodule:: dbgdb.ogr.postgres
.. moduleauthor:: Pat Daburu <pat@daburu.net>

This module contains functions
"""
# import json
# from pathlib import Path
from urllib.parse import urlparse, ParseResult
# from addict import Dict
# import psycopg2
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path
import subprocess
from . import OGR2OGR
from ..db.postgres import (
    create_db,
    create_extensions,
    create_schema,
    db_exists,
    select_schema_tables
)


def load(gdb: Path,
         url: str = 'postgresql://postgres@localhost:5432/postgres',
         schema: str = 'imports',
         overwrite: bool = True,
         progress: bool = True,
         use_copy: bool = True):
    """
    Load a file geodatabase (GDB) into a Postgres database.

    :param gdb: the path to the file geodatabase
    :param url: the URL of the Postgres instance
    :param schema: the target schema
    :param overwrite: Overwrite existing data?
    :param progress: Show progress?
    :param use_copy: Use COPY instead of INSERT when loading?
    """
    # Parse the URL.  (We'll need the pieces to construct an ogr2ogr connection
    # string.)
    dbp: ParseResult = urlparse(url)
    # Grab the proposed database name.
    dbname: str = dbp.path[1:]
    # If the target database doesn't exist...
    if not db_exists(url=url, dbname=dbname):
        # ...let's try to create it.
        create_db(url=url, dbname=dbname)
    # Before we let OGR do its thing, we need to make sure the database is
    # ready.
    create_extensions(url=url)
    create_schema(url=url, schema=schema)

    # Let's start putting the command string together.
    cmd = [
        OGR2OGR,
        '-f', 'PostgreSQL',
        f"PG:host='{dbp.hostname}' user='{dbp.username}' dbname='{dbname}' "
        f"port='{dbp.port}'",
        '-lco', f'SCHEMA={schema}'
    ]
    # If we're overwriting...
    if overwrite:
        cmd.extend(['-lco', 'OVERWRITE=YES'])
    # If we're supposed to show progress...
    if progress:
        cmd.append('-progress')
    # Should we use COPY instead of INSERT?
    # See https://trac.osgeo.org/gdal/wiki/ConfigOptions#PG_USE_COPY
    if use_copy:
        cmd.extend(['--config', 'PG_USE_COPY', 'YES'])
    # Lastly, add the target geodatabase.
    cmd.append(str(gdb))
    # https://gis.stackexchange.com/questions/154004/execute-ogr2ogr-from-python
    subprocess.check_call(cmd)


def extract(gdb: Path,
            url: str = 'postgresql://postgres@localhost:5432/postgres',
            schema: str = 'imports'):
    """
    Extract a schema from a PostgreSQL database to a file geodatabase.

    :param gdb: the path to the output GDB file
    :param url: the URL of the Postgres database instance
    :param schema: the schema to export
    """
    # Parse the URL.  (We'll need the pieces to construct an ogr2ogr connection
    # string.)
    dbp: ParseResult = urlparse(url)
    # Grab the proposed database name.
    dbname: str = dbp.path[1:]
    # Let's put the command together.
    cmd = [
        OGR2OGR,
        '-f', 'FileGDB',
        str(gdb),
        f"PG:host='{dbp.hostname}' user='{dbp.username}' dbname='{dbname}' "
        f"port='{dbp.port}'"
    ]
    # Add the names of all the tables in the target schema.
    cmd.extend([
        f'{schema}.{table_name}'
        for table_name
        in select_schema_tables(url=url, schema=schema)
    ])
    # Go! Go! Go!
    subprocess.check_call(cmd)

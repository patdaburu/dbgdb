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
from ..db.postgres import create_db, create_extensions, create_schema, db_exists


# # Load the Postgres phrasebook.
# sql_phrasebook = Dict(
#     json.loads(
#         (
#                 Path(__file__).resolve().parent / 'postgres.json'
#         ).read_text()
#     )['sql']
# )
#
#
# def connect(url: str, dbname=None):
#     """
#     Create a connection to a Postgres database.
#
#     :param url: the Postgres instance URL
#     :param dbname: the target database name (if it differs from the one
#         specified in the URL)
#     :return: a psycopg2 connection
#     """
#     # Parse the URL.  (We'll need the pieces to construct an ogr2ogr connection
#     # string.)
#     db: ParseResult = urlparse(url)
#     # TODO: This requires much error handling.
#     cnx_opt = {
#         k: v for k, v in
#         {
#             'host': db.hostname,
#             'port': int(db.port),
#             'database': dbname if dbname is not None else db.path[1:],
#             'user': db.username,
#             'password': db.password
#         }.items() if v is not None
#     }
#     return psycopg2.connect(**cnx_opt)
#
#
# def db_exists(url: str,
#               dbname: str = None,
#               admindb:str = 'postgres') -> bool:
#     """
#     Does a given database on a Postgres instance exist?
#
#     :param url: the Postgres instance URL
#     :param dbname: the name of the database to test
#     :param admindb: the name of an existing (presumably the main) database
#     :return: `True` if the database exists, otherwise `False`
#     """
#     # Let's see what we got for the database name.
#     _dbname = dbname
#     # If the caller didn't specify a database name...
#     if not _dbname:
#         # ...let's figure it out from the URL.
#         db: ParseResult = urlparse(url)
#         _dbname = db.path[1:]
#     # Now, let's do this!
#     with connect(url=url, dbname=admindb) as cnx:
#         with cnx.cursor() as crs:
#             # Execute the SQL query that counts the databases with a specified
#             # name.
#             crs.execute(
#                 sql_phrasebook.select_db_count.format(_dbname)
#             )
#             # If the count isn't zero (0) the database exists.
#             return crs.fetchone()[0] != 0
#
#
# def schema_exists(url: str, schema: str):
#     """
#     Does a given schema exist within a Postgres database?
#
#     :param url: the Postgres instance URL and database
#     :param schema: the name of the schema
#     :return: `True` if the schema exists, otherwise `False`
#     """
#     # If the database specified in the URL doesn't exist...
#     if not db_exists(url=url):
#         # ...it stands to reason that the schema cannot exist.
#         return False
#     # At this point, it looks as thought database exists, so let's check for
#     # the schema.
#     with connect(url=url) as cnx:
#         with cnx.cursor() as crs:
#             # Execute the SQL query that counts the schemas with a specified
#             # name.
#             crs.execute(
#                 sql_phrasebook.select_schema_count.format(schema)
#             )
#             # If the count isn't zero (0) the database exists.
#             return crs.fetchone()[0] != 0
#
#
# def drop_schema(url: str, schema: str):
#     with connect(url=url) as cnx:
#         with cnx.cursor() as crs:
#             # Execute the SQL query that counts the schemas with a specified
#             # name.
#             crs.execute(
#                 sql_phrasebook.drop_schema.format(schema)
#             )
#
#
# def create_db(
#         url: str,
#         dbname: str,
#         admindb: str='postgres'):
#     """
#     Create a database on a Postgres instance.
#
#     :param url: the Postgres instance URL
#     :param dbname: the name of the database
#     :param admindb: the name of an existing (presumably the main) database
#     :return:
#     """
#     with connect(url=url, dbname=admindb) as cnx:
#         cnx.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
#         with cnx.cursor() as crs:
#             crs.execute(sql_phrasebook.create_db.format(dbname))


def load(
        gdb: Path,
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
    db: ParseResult = urlparse(url)
    # Grab the proposed database name.
    dbname: str = db.path[1:]
    # If the target database doesn't exist...
    if not db_exists(url=url, dbname=dbname):
        # ...let's try to create it.
        create_db(url=url, dbname=dbname)  # TODO: force should be a user parameter.
    # Before we let OGR do its thing, we need to make sure the database is
    # ready.
    create_extensions(url=url)
    create_schema(url=url, schema=schema)

    # Let's start putting the command string together.
    cmd = [
        OGR2OGR,
        '-f', 'PostgreSQL',
        f"PG:host='{db.hostname}' user='{db.username}' dbname='{dbname}' "
        f"port='{db.port}'",
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

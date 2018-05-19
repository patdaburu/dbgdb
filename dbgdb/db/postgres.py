#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created by pat on 5/19/18
"""
.. currentmodule:: postgres
.. moduleauthor:: Pat Daburu <pat@daburu.net>

This module contains utility functions to help when working with PostgreSQL
databases.
"""
import json
from pathlib import Path
from urllib.parse import urlparse, ParseResult
from addict import Dict
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


# Load the Postgres phrasebook.
sql_phrasebook = Dict(
    json.loads(
        (
                Path(__file__).resolve().parent / 'postgres.json'
        ).read_text()
    )['sql']
)


def connect(url: str, dbname=None, autocommit: bool=False):
    """
    Create a connection to a Postgres database.

    :param url: the Postgres instance URL
    :param dbname: the target database name (if it differs from the one
        specified in the URL)
    :param autocommit: Set the `autocommit` flag on the connection?
    :return: a psycopg2 connection
    """
    # Parse the URL.  (We'll need the pieces to construct an ogr2ogr connection
    # string.)
    db: ParseResult = urlparse(url)
    # TODO: This requires much error handling.
    cnx_opt = {
        k: v for k, v in
        {
            'host': db.hostname,
            'port': int(db.port),
            'database': dbname if dbname is not None else db.path[1:],
            'user': db.username,
            'password': db.password
        }.items() if v is not None
    }
    cnx = psycopg2.connect(**cnx_opt)
    # If the caller requested that the 'autocommit' flag be set...
    if autocommit:
        # ...do that now.
        cnx.autocommit = True
    return cnx


def db_exists(url: str,
              dbname: str = None,
              admindb:str = 'postgres') -> bool:
    """
    Does a given database on a Postgres instance exist?

    :param url: the Postgres instance URL
    :param dbname: the name of the database to test
    :param admindb: the name of an existing (presumably the main) database
    :return: `True` if the database exists, otherwise `False`
    """
    # Let's see what we got for the database name.
    _dbname = dbname
    # If the caller didn't specify a database name...
    if not _dbname:
        # ...let's figure it out from the URL.
        db: ParseResult = urlparse(url)
        _dbname = db.path[1:]
    # Now, let's do this!
    with connect(url=url, dbname=admindb) as cnx:
        with cnx.cursor() as crs:
            # Execute the SQL query that counts the databases with a specified
            # name.
            crs.execute(
                sql_phrasebook.select_db_count.format(_dbname)
            )
            # If the count isn't zero (0) the database exists.
            return crs.fetchone()[0] != 0


def create_schema(url: str, schema: str):
    with connect(url=url) as cnx:
        with cnx.cursor() as crs:
            crs.execute(sql_phrasebook.create_schema.format(schema))


def schema_exists(url: str, schema: str):
    """
    Does a given schema exist within a Postgres database?

    :param url: the Postgres instance URL and database
    :param schema: the name of the schema
    :return: `True` if the schema exists, otherwise `False`
    """
    # If the database specified in the URL doesn't exist...
    if not db_exists(url=url):
        # ...it stands to reason that the schema cannot exist.
        return False
    # At this point, it looks as thought database exists, so let's check for
    # the schema.
    with connect(url=url) as cnx:
        with cnx.cursor() as crs:
            # Execute the SQL query that counts the schemas with a specified
            # name.
            crs.execute(
                sql_phrasebook.select_schema_count.format(schema)
            )
            # If the count isn't zero (0) the database exists.
            return crs.fetchone()[0] != 0


def drop_schema(url: str, schema: str):
    with connect(url=url, autocommit=True) as cnx:
        with cnx.cursor() as crs:
            # Execute the SQL query that counts the schemas with a specified
            # name.
            crs.execute(
                sql_phrasebook.drop_schema.format(schema)
            )


def create_db(
        url: str,
        dbname: str,
        admindb: str='postgres'):
    """
    Create a database on a Postgres instance.

    :param url: the Postgres instance URL
    :param dbname: the name of the database
    :param admindb: the name of an existing (presumably the main) database
    :return:
    """
    with connect(url=url, dbname=admindb) as cnx:
        cnx.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with cnx.cursor() as crs:
            crs.execute(sql_phrasebook.create_db.format(dbname))


def create_extensions(url: str):
    with connect(url=url, autocommit=True) as cnx:
        with cnx.cursor() as crs:
            # Make sure the extensions are installed.
            for sql in sql_phrasebook.create_extensions:
                crs.execute(sql)

#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created by pat on 5/18/18
"""
.. currentmodule:: dbgdb.targets.postgres
.. moduleauthor:: Pat Daburu <pat@daburu.net>

This module contains PostgreSQL targets.
"""
from pathlib import Path
import luigi
from ..ogr.postgres import connect, schema_exists


class PgSchemaTarget(luigi.Target):
    """
    This is a target that represents a file geodatabase (GDB).
    """
    def __init__(self, url: str, schema: str):
        """

        :param url: the path to the file GDB
        :param schema: the target schema
        """
        super().__init__()
        self._url: str = url
        self._schema: str = schema

    def exists(self) -> bool:
        """
        Does the file target schema exist?

        :return: `True` if the file geodatabase exists, otherwise `False`
        """
        return schema_exists(url=self._url, schema=self._schema)

    def connect(self):
        """
        Get a connection to the database.

        :return: a connection to the database
        """
        return connect(url=self._url)

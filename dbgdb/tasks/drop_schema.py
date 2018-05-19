#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created by pat on 5/19/18
"""
.. currentmodule:: drop_db
.. moduleauthor:: Pat Daburu <pat@daburu.net>

This module contains the :py:class:`DropDbTask` task which you can use to
drop a database.
"""
import luigi
from ..targets.postgres import PgSchemaTarget
from ..ogr.postgres import drop_schema


class DropSchemaTask(luigi.Task):
    """
    This task loads a file geodatabase into a database instance.

    :cvar url: the URL of the target database
    :cvar admindb: the target schema
    """
    url: luigi.Parameter = luigi.Parameter(
        default='postgresql://postgres@localhost:5432/postgres',
        description='the URL of the database instance'
    )
    schema: luigi.Parameter = luigi.Parameter(
        description='the schema name')

    def requires(self):
        """
        This task has no requirements.

        :return: an empty iteration
        """
        return []

    def output(self) -> PgSchemaTarget:
        """
        This task returns a :py:class:`PgSchemaTarget` that points to the
        target schema where the GDB was loaded.

        :return: the PostgreSQL schema target
        """
        # Note the use of the 'dne' flag in the target constructor:  We want to
        # say this target exists only if the schema *does not* exist.
        return PgSchemaTarget(url=str(self.url),
                              schema=str(self.schema),
                              dne=True)

    def run(self):
        """
        Run the task.
        """
        print("OK I AM DROPPING IT!")
        drop_schema(url=str(self.url), schema=str(self.schema))
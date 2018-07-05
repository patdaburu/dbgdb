#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: test_drop
.. moduleauthor:: Pat Daburu pat@daburu.net

This is an integration test module for the :py:class:`PgDropTask`.
"""

import unittest
import luigi.mock
import luigi.worker
import testing.postgresql
from dbgdb.db.postgres import create_schema, schema_exists
from dbgdb.tasks.postgres.drop import PgDropSchemaTask

# To learn more about mocking Luigi objects, visit the link below.
# http://luigi.readthedocs.io/en/stable/api/luigi.mock.html


class PgDropTaskTestSuite(unittest.TestCase):
    """
    This class contains tests of the :py:class:`PgDropTask`.
    """
    pgdb = None  #: the temporary Postgres database instance
    test_schema = 'test'  #: the name of the db schema for import/export

    def setUp(self):
        """
        Initialize the temporary PostgreSQL instance.
        """
        self.pgdb = testing.postgresql.Postgresql()

    def tearDown(self):
        """
        Shut down the temporary PostgreSQL instance and clean up the temporary
        working directory.
        """
        self.pgdb.stop()

    def test_drop(self):
        """
        Create a schema in the database and use a :py:class:`PgDropTask` to
        remove it.
        """
        url = self.pgdb.url()
        # Manually create the test schema.
        create_schema(url=url, schema=self.test_schema)
        # Verify the test schema exists.
        self.assertTrue(schema_exists(url=url, schema=self.test_schema))
        # Run the task to drop the schema.
        worker = luigi.worker.Worker()
        worker.add(PgDropSchemaTask(url=url, schema=self.test_schema))
        worker.run()
        # Verify the schema is gone.
        self.assertFalse(schema_exists(url=url, schema=self.test_schema))


if __name__ == '__main__':
    unittest.main()

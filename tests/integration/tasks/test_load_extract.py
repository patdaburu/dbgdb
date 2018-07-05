#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: test_load_extract
.. moduleauthor:: Pat Daburu pat@daburu.net

This is an integration test module for the :py:class:`PgLoadTask` and
:py:class:`PgExtractTask`.
"""

from pathlib import Path
import unittest
import shutil
import tempfile
import zipfile
import luigi.mock
import luigi.worker
import testing.postgresql
from dbgdb.db.postgres import schema_exists
from dbgdb.ogr.postgres import OgrDrivers
from dbgdb.tasks.postgres.extract import PgExtractTask
from dbgdb.tasks.postgres.load import PgLoadTask

# To learn more about mocking Luigi objects, visit the link below.
# http://luigi.readthedocs.io/en/stable/api/luigi.mock.html


class PgLoadExtractTaskTestSuite(unittest.TestCase):
    """
    This class contains tests that load sample data into a test Postgres
    instance, then extract the data to files.
    """
    pgdb = None  #: the temporary Postgres database instance
    temp_dir = tempfile.mkdtemp()  #: the temporary working directory
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
        shutil.rmtree(self.temp_dir)

    def test_loadExtract(self):
        """
        Load data into a database using a :py:class:`PgLoadTask` and extract
        it using a :py:class:`PgExtractTask`.
        """
        # Where is the test data?
        test_data_zip_path = \
            Path(__file__).resolve().parent.parent.parent / 'data/test.gdb.zip'

        # Create a temporary directory in which to place files.
        temp_dir = tempfile.mkdtemp()
        # Extract the test data.
        with zipfile.ZipFile(str(test_data_zip_path), 'r') as test_data_zip:
            test_data_zip.extractall(temp_dir)
        # Now, figure out where the test data resides.
        test_gdb_path = str(Path(temp_dir) / 'test.gdb')

        # PART ONE: Run the 'load' task.
        load_worker = luigi.worker.Worker()
        load_task = PgLoadTask(
            url=self.pgdb.url(),
            schema='test',
            indata=test_gdb_path,
        )
        load_worker.add(load_task)
        load_worker.run()
        # Get the load task's target.
        load_task_target = load_task.output()
        # Verify that the load task's target claims it exists.
        self.assertTrue(
            load_task_target.exists(),
            msg="The load task's target does not exist."
        )
        # Double-check the target's assertion.
        self.assertTrue(
            schema_exists(
                url=self.pgdb.url(),
                schema=self.test_schema
            ),
            msg=f'After import the {self.test_schema} schema was not found.'
        )

        # PART TWO: Run the 'extract' task.
        outdata_prefix = str(Path(self.temp_dir) / 'output')
        for driver in [OgrDrivers.Spatialite]:
            # The path to the output file will use the driver enumeration
            # value as its extension.
            outdata = f'{Path(outdata_prefix)}.{driver.value}'
            # Run the export task.
            extract_worker = luigi.worker.Worker()
            extract_task = PgExtractTask(
                url=self.pgdb.url(),
                schema='test',
                outdata=str(outdata),
                driver=driver
            )
            extract_worker.add(extract_task)
            extract_worker.run()
            # Get the extract task's target.
            extract_task_target = extract_task.output()
            # Verify the extract task's target claims it exists.
            self.assertTrue(
                extract_task_target.exists(),
                msg="The extract task's target does not exist"
            )
            # Double-check the extract task target's claim.
            self.assertTrue(
                Path.exists(Path(outdata)),
                msg=f'After export {outdata} was not created.'
            )


if __name__ == '__main__':
    unittest.main()

#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. currentmodule:: test_tasks.py
.. moduleauthor:: Pat Daburu pat@daburu.net

This is the test module for loading tasks.
"""

from pathlib import Path
import unittest
import shutil
import tempfile
import zipfile
import luigi.mock
import luigi.worker
from mock import patch
import testing.postgresql
from dbgdb.db.postgres import schema_exists
from dbgdb.ogr.postgres import OgrDrivers
from dbgdb.tasks.postgres.extract import PgExtractTask
from dbgdb.tasks.postgres.load import PgLoadTask


# To learn more about mocking Luigi objects, visit the link below.
# http://luigi.readthedocs.io/en/stable/api/luigi.mock.html


class PgLoadTaskTestSuite(unittest.TestCase):

    def test_createTempDb_load_noErrors(self):
        """
        Arrange: Create a temporary database.
        Act: Load the data model.
        Assert: No errors occurred.
        """
        # Where is the test data?
        test_data_zip_path = (
                Path(__file__).resolve().parent.parent / 'data/test.gdb.zip'
        )
        # Create a temporary directory in which to place files.
        temp_dir = tempfile.mkdtemp()
        # Extract the test data.
        with zipfile.ZipFile(str(test_data_zip_path), 'r') as test_data_zip:
            test_data_zip.extractall(temp_dir)
        # Now, figure out where the test data resides.
        test_gdb_path = str(Path(temp_dir) / 'test.gdb')
        # Create the temporary database.
        pgdb = testing.postgresql.Postgresql()
        try:
            # PART ONE: Run the 'load' task.
            load_worker = luigi.worker.Worker()
            load_worker.add(PgLoadTask(
                url=pgdb.url(),
                schema='test',
                indata=test_gdb_path,
            ))
            load_worker.run()
            # Make sure the schema was created in the database.
            self.assertTrue(schema_exists(url=pgdb.url(), schema='test'))

            # PART TWO: Run the 'extract' task.
            outdata_prefix = str(Path(temp_dir) / 'output')
            for driver in [OgrDrivers.Spatialite]:
                # The path to the output file will use the driver enumeration
                # value as its extension.
                outdata = f'{Path(outdata_prefix)}.{driver.value}'
                extract_worker = luigi.worker.Worker()
                extract_worker.add(PgExtractTask(
                    url=pgdb.url(),
                    schema='test',
                    outdata=str(outdata),
                    driver=driver
                ))
                extract_worker.run()
                self.assertTrue(
                    Path.exists(Path(outdata)),
                    msg=f'File {outdata} was not created.'
                )

        finally:
            pgdb.stop()
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()

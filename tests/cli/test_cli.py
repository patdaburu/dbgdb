#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created by pat on 7/5/18
"""
.. currentmodule:: test_cli
.. moduleauthor:: Pat Daburu <pat@daburu.net>

This is a unit test module for the command-line application.  Calls to
:py:func:`luigi.build` are mocked so no tasks are actually run.
"""

from pathlib import Path
import tempfile
import unittest
from click.testing import CliRunner
from mock import patch
import dbgdb.cli

class CliTestSuite(unittest.TestCase):
    """
    This class contains tests of the command-line application's commands.  Calls
    to :py:func:`luigi.build` are mocked so no tasks are actually run.
    """
    indata = str(
        Path(__file__).resolve().parent.parent / 'data/test.gdb.zip'
    )  #: the path to input data

    @patch('luigi.build')
    def test_load(self, _):
        """
        Test the :py:func:`dbgdb.cli.load` sub-command.
        :param _: the mocked :py:func:`luigi.build` function
        """
        runner = CliRunner()
        result = runner.invoke(dbgdb.cli.cli, ['load', self.indata])
        self.assertEqual(0, result.exit_code)

    @patch('luigi.build')
    def test_extract(self, _):
        """
        Test the :py:func:`dbgdb.cli.extract` sub-command.
        :param _: the mocked :py:func:`luigi.build` function
        """
        # Pick a temporary file name for output.
        outdata = tempfile.NamedTemporaryFile().name
        runner = CliRunner()
        result = runner.invoke(dbgdb.cli.cli, ['extract', outdata])
        print(result.output)
        self.assertEqual(0, result.exit_code)

    @patch('luigi.build')
    def test_drop(self, _):
        """
        Test the :py:func:`dbgdb.cli.drop` sub-command.
        :param _: the mocked :py:func:`luigi.build` function
        """
        runner = CliRunner()
        result = runner.invoke(dbgdb.cli.cli, ['drop', 'schema', 'test'])
        self.assertEqual(0, result.exit_code)

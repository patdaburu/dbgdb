#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created by pat on 5/18/18
"""
.. currentmodule:: dbgdb.targets
.. moduleauthor:: Pat Daburu <pat@daburu.net>

This module contains custom file geodatabase (GDB) targets.
"""
from pathlib import Path
import luigi


class FileGdbTarget(luigi.Target):
    """
    This is a target that represents a file geodatabase (GDB).
    """
    def __init__(self, path: Path):
        """

        :param path: the path to the file GDB
        """
        super().__init__()
        self._path = path

    def path(self) -> Path:
        """
        Get the path to the file geodatabase.

        :return: the path to the file geodatabase
        """
        return self._path

    def exists(self) -> bool:
        """
        Does the file geodatabase exist?

        :return: `True` if the file geodatabase exists, otherwise `False`
        """
        return self._path.exists()


# class MockFileGdbTarget(FileGdbTarget):
#     def __init__(self, path: Path, exists: bool):
#         super().__init__(path=path)
#         self._exists: bool = exists
#
#     def exists(self) -> bool:
#         return self._exists

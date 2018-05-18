#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Created by pat on 5/16/18
"""
.. currentmodule:: dbgdb.ogr
.. moduleauthor:: Pat Daburu <pat@daburu.net>

`OGR <https://trac.osgeo.org/gdal/wiki/FAQGeneral#WhatdoesOGRstandfor>`_
used to stand for OpenGIS Simple Features Reference Implementation. However,
since OGR is not fully compliant with the OpenGIS Simple Feature specification
and is not approved as a reference implementation of the spec the name was
changed to OGR Simple Features Library. The only meaning of OGR in this name is
historical. OGR is also the prefix used everywhere in the source of the library
for class names, filenames, etc.

Interesting, right?
"""
from enum import Enum
import os
from pathlib import Path
import subprocess
import urllib.parse as urlparse
from typing import NamedTuple

OGR2OGR = 'ogr2ogr'  #: the ogr2ogr executable


class DatabaseFormats(Enum):
    POSTGRESQL = 'PostgreSQL'

    @staticmethod
    def get_prefix(self, format: 'DatabaseFormats'):
        return {
            self.POSTGRESQL: 'PG'
        }[format]




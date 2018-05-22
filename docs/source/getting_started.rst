.. _getting-started:

.. image:: _static/images/logo.svg
   :width: 80px
   :alt: dbgdb
   :align: right

.. toctree::
    :glob:

Getting Started
===============

Prerequisites
-------------

Installing the Library
^^^^^^^^^^^^^^^^^^^^^^

You can use pip to install `dbgdb`.

.. code-block:: sh

    pip install dbgdb

GDAL/OGR
^^^^^^^^

You will need to have `ogr2ogr` installed.  You can arrange this by installing
`GDAL <http://www.gdal.org/>`_.

.. note::

    In this early version we expect `ogr2ogr` to be in your system path.
    Improvements on that point are forthcoming.

Using Tasks
-----------

This library contains a number of
`Luigi tasks <http://luigi.readthedocs.io/en/stable/tasks.html>`_ that you can
use in your own pipelines.  These include:

* :py:class:`dbgdb.tasks.postgres.load.PgLoadTask`
    for loading data (like file geodatabases) into your PostgreSQL database;
* :py:class:`dbgdb.tasks.postgres.extract.PgExtractTask`
    for extrating data from your PostgreSQL database; and
* :py:class:`dbgdb.tasks.postgres.drop.PgDropSchemaTask`
    if you need to drop an import schema because you're starting over.


Using the Command Line
----------------------
Most of the commands you run with the command-line interface (CLI) create Luigi
tasks which are then submitted to a Luigi scheduler.

.. note::

    The `-l` parameter indicates that the tasks should be run using the
    `local scheduler <http://luigi.readthedocs.io/en/stable/central_scheduler.html#>`_.
    The examples listed below use this parameter.  If you want to submit tasks
    to the Luigi daemon
    (`luigid <http://luigi.readthedocs.io/en/stable/central_scheduler.html#the-luigid-server>`_)
    you can simply omit this parameter.

Getting Help
------------

`dbgdb` has its own command-line help.

.. code-block:: sh

    dbgdb --help

Loading Data
^^^^^^^^^^^^

You can load a file geodatabase with the `load` subcommand.

.. code-block:: sh

    dbgdb -l load --schema myschema /path/to/your/data.gdb


Extracting Data
^^^^^^^^^^^^^^^

You can extract all of the data within a schema to an output file with the
`extract` subcommand.

.. code-block:: sh

    dbgdb -l extract  --schema myschema /path/to/your/exported/data.db

.. note::

    At the moment, we can export to
    `GeoPackage <https://www.geopackage.org/>`_ or
    `Spatialite <https://www.geopackage.org/>`_ formats.  Support for ESRI
    File Geodatabases (gdb) is still in the works.


Dropping a Schema
^^^^^^^^^^^^^^^^^

If the target schema for your load command already exists, you may notice
Luigi reporting there was nothing to do because, from the task's perspective,
the work has already been done.  If you need to drop a schema, you can use the
`drop` subcommand.

.. code-block:: sh

    dbgdb -l drop schema myschema

Resources
---------

Would you like to learn more?  Check out the links below.

* `Luigi <http://luigi.readthedocs.io/en/stable/index.html>`_
* `Click <http://click.pocoo.org/5/>`_

# dbgdb

This is a library containing [Luigi](http://luigi.readthedocs.io/en/stable/index.html) tasks to help you get that data into (and out of) your GIS databases.  The library also comes with a command-line interface based on [Click](http://click.pocoo.org/5/) that can be helpful for running tasks individually.

## Getting Started


### Prerequisites

#### pip install
You can use pip to install the library itself.

```bash
pip install dbgdb
```

#### GDAL/OGR
You will also need `ogr2ogr` which you can get by installing [GDAL](http://www.gdal.org/).

**Note: In this early version we expect `ogr2ogr` to be in your system path.  Improvements on that point are forthcoming.**


## Using Tasks

This library contains a number of tasks that you can use in your Luigi pipelines.

### PgImportTask

Use `PgImportTask` task to import your data (likely a file geodatabase) to your PostgreSQL database.

### PgExtractTask

Use a `PgExtractTask` to get your data out of the database.

### PgDropSchemaTask

If you need to drop the import schema because you're starting the process over, use a `PgDropSchemaTask`.

## Using the Command Line

### A Note about the CLI and Luigi

Most of the commands you run with the command-line interface (CLI) create Luigi tasks which are then submitted to a Luigi scheduler.  The *-l* parameter indicates that the tasks should be run using the local scheduler.  The examples listed below use this parameter.

### Getting Help
`dbgdb` has its own command-line help.

```bash
dbgdb --help
```

### Load Data

You can load a file geodatabase with the `load` subcommand.
```bash
dbgdb -l load --schema myschema /path/to/your/data.gdb
```

### Extract Data

You can extract all of the data within a schema to an output file.
```bash
dbgdb -l extract  --schema myschema /path/to/your/exported/data.db
```
**Note: At the moment, we can export to [GeoPackage](https://www.geopackage.org/) or [Spatialite](https://www.gaia-gis.it/fossil/libspatialite/index) formats.  Support for ESRI File Geodatabses (gdb) is still in the works.**

### Drop a Schema

If the target schema for your `load` command already exists, you may notice Luigi reports there was nothing to do because, from the task's perspective, the work is already done.  If you need to drop a schema, you can use the `drop` subcommand.

```bash
dbgdb -l drop schema myschema
```


## Resources

Would you like to learn more?  Check out the links below!

*  [Luigi](http://luigi.readthedocs.io/en/stable/index.html)
*  [Click](http://click.pocoo.org/5/)

## Authors

* **Pat Daburu** - *Initial work* - [github](https://github.com/patdaburu)

See also the list of [contributors](https://github.com/cookiecutter-modlit/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

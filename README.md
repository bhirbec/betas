The goal of this project is to develop scalable, reusable, extensible code that estimates the beta 
for a basket of individual stocks and visualizes the results using the Python programming language 
and supporting libraries.

# Install Development Environment (Linux)

Download and install [Anaconda2-4.0](https://www.continuum.io/downloads) for Linux.

# Running the ETL

Run the ETL with the following command:

`$ python etl.py`

Some options are available as command line arguments:

```
$ python etl.py -h
Usage: python etl.py [options]

Download historical prices from Yahoo Finance and compute some financial
indicators like stock Beta. The first ETL run will create a PyTables database.
Subsequent runs will only download the historical data generated between the previous
run and the current date (or `end_date` if specified).

Options:
  -h, --help            show this help message and exit
  --db-path=DB_PATH     Path to the PyTables file
  --start-date=START_DATE
                        Download history as of this date (yyyy-mm-dd). It has
                        no effect if the database already exists (default
                        2010-01-01)
  --end-date=END_DATE   Download history up to this date (yyyy-mm-dd). Default
                        to the current date
  --destroy             Destroy the PyTables file which forces all the stocks
                        to be downloaded since the `start_date`
  --no-download         Do not download any data from Yahoo - just compute
                        indicators
  --nb-greenthreads=NB_GREENTHREADS
                        Number of greenthreads used to download data
  --nb-proc=NB_PROC     Number of processors used to compute indicators
                        (default to multiprocessing.cpu_count())
```

# Running the Unit Tests

Run the test with the following command:

`$ nosetests test/tests.py`

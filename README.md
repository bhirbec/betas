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
Usage: etl.py [options]

Download historical prices from Yahoo Finance and compute some financial indicators like stock Beta.

Options:
  -h, --help            show this help message and exit
  -d DB_PATH, --db-path=DB_PATH
                        Path to the PyTables file
  -s START_DATE, --start-date=START_DATE
                        Download history as of this date (yyyy-mm-dd)
```

# Running the Unit Tests

Run the test with the following command:

`$ nosetests etl_test.py`

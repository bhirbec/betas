# Beta Analysis

The goal of this project is to develop scalable, reusable, extensible code that estimates the beta 
for a basket of individual stocks and visualizes the results using the Python programming language 
and supporting libraries.

This project is composed of two scripts:

- etl/etl.py: imports about 3000 stocks (NASDAQ only) from Yahoo Finance and computes rolling
  betas (30 days) for each stock. 

- web/web.py: GUI that makes possible to show the rolling betas for each stock. It is
  build with React.js

Demo available here [http://104.197.68.179/](http://104.197.68.179/) (Tested under Google Chrome only)

## Development Environment

Download and install [Anaconda2-4.0](https://www.continuum.io/downloads).

Clone the repository:

`$ git clone https://github.com/bhirbec/betas.git`

Create Anaconda project:

`$ conda create --name betas`

Install `eventlet` with pip:

```
$ source activate betas
$ pip install eventlet
```

## Running the ETL

Run the ETL with the following command:

`$ python etl/etl.py`

Some options are available:

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

## Running the Web Server

Start the server with the following command:

`$ PYTHONPATH=. python web/web.py`

Point your browser (only tested on Google Chrome) at [localhost:8080](http://localhost:8080/).

Here's the full description of the command:

```
$ PYTHONPATH=. python web/web.py
Usage: python web.py [options]

Start a web server that provides Financial reports.


Options:
  -h, --help         show this help message and exit
  --host=HOST        TCP Host (default: localhost)
  --port=PORT        TCP port (default: 8080)
  --db-path=DB_PATH  Path to the PyTables file
```

## Running the Unit Tests

Run the test with the following command:

`$ nosetests test/tests.py`

## Deployment

Install [Ansible](http://docs.ansible.com/ansible/intro_installation.html) and run this playbook:

`$ ansible-playbook ansible/deploy.yaml -i ansible/gce_hosts`  

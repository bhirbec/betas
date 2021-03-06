import sys
import os
import time
from datetime import datetime
from optparse import OptionParser
from multiprocessing import cpu_count

from loader import load_data
from indicator import compute_indicators
from dblib import Storage, parse_date


PROJECT_DIR = os.path.dirname(os.path.realpath(__file__))

MARKETS = [
    (os.path.join(PROJECT_DIR, '../data/nasdaq.csv'), '^IXIC'),
    (os.path.join(PROJECT_DIR, '../data/nyse.csv'), '^NYA')
]


parser = OptionParser(usage=(
    'usage: python %prog [options]\n\n'
    'Download historical prices from Yahoo Finance and compute some financial\n'
    'indicators like stock Beta. The first ETL run will create a PyTables database.\n'
    'Subsequent runs will only download the historical data generated between the previous\n'
    'run and the current date (or `end_date` if specified).'
))

parser.add_option('--db-path',
                  dest='db_path',
                  default='db.h5',
                  help='Path to the PyTables file')

parser.add_option('--start-date',
                  dest='start_date',
                  default='2010-01-01',
                  help='Download history as of this date (yyyy-mm-dd). It has no effect '
                        'if the database already exists (default 2010-01-01)')

parser.add_option('--end-date',
                  dest='end_date',
                  help='Download history up to this date (yyyy-mm-dd). Default to the current date')

parser.add_option('--destroy',
                  dest='destroy',
                  action='store_true',
                  help='Destroy the PyTables file which forces all the stocks to be downloaded since '
                       'the `start_date`')

parser.add_option('--no-download',
                  dest='no_download',
                  action='store_true',
                  help='Do not download any data from Yahoo - just compute indicators')

parser.add_option('--nb-greenthreads',
                  dest='nb_greenthreads',
                  type='int',
                  default=10,
                  help='Number of greenthreads used to download data')

parser.add_option('--nb-proc',
                  dest='nb_proc',
                  type='int',
                  default=None,
                  help='Number of processors used to compute indicators (default to '
                       'multiprocessing.cpu_count())')

parser.add_option('-n', '--nb-stocks',
                  dest='nb_stocks',
                  type='int',
                  default=None,
                  help='Limit the download to `n` stocks')


def main(options):
    try:
        start_time = time.time()
        store = Storage(options.db_path)
        load_data(store, MARKETS, options)
        compute_indicators(store, MARKETS, options.nb_proc)
        print 'Total work took %s' % (time.time() - start_time)
    finally:
        store.close()


if __name__ == '__main__':
    options, args = parser.parse_args()

    db_dir = os.path.dirname(options.db_path) or '.'
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    elif os.path.exists(options.db_path) and options.destroy:
        os.remove(options.db_path)

    start_date = parse_date(options.start_date)
    if start_date is None:
        parser.print_help()
        sys.exit("Wrong parameter `start_end`: %s" % options.start_date)

    end_date = None
    if options.end_date:
        end_date = parse_date(options.end_date)
        if end_date is None:
            parser.print_help()
            sys.exit("Wrong parameter `end_date`: %s" % options.end_date)

    options.start_date = start_date
    options.end_date = end_date
    options.nb_proc = options.nb_proc or cpu_count()
    main(options)

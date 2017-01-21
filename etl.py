import sys
import csv
import os
import time
from datetime import datetime, timedelta
from optparse import OptionParser
from urllib import urlencode
from urllib2 import urlopen

import numpy as np
from numpy import float64
from tables import open_file, IsDescription, StringCol, Float64Col


NASDAQ = '^IXIC'
APPLE = 'AAPL'


class StockHistory(IsDescription):
    date    = StringCol(10, pos=1)
    opening = Float64Col(pos=2)
    closing = Float64Col(pos=3)
    volume  = Float64Col(pos=4)
    roi     = Float64Col(pos=5)


def main(options):
    start = time.time()

    h5file = open_file(options.db_path, mode = "w")
    group = h5file.create_group("/", 'history', 'Historical stock prices')

    # TODO: write are not thread safe. Need to serialize writes.
    for symbol in [NASDAQ, APPLE]:
        update_stock_history(h5file, group, symbol, options.start_date, options.end_date)

    table = get_table(h5file, '/history/' + APPLE)
    stock = table.read()
    table.close()

    table = get_table(h5file, '/history/' + NASDAQ)
    bench = table.read()
    table.close()

    # These two series are identical in terms of time frame, number of days! This
    # assumption may not hold with other stocks.
    start_beta = time.time()
    compute_running_betas(stock, bench)
    print 'beta computation took %s' % (time.time() - start_beta)
    print 'Total work took %s' % (time.time() - start)

    table.close()
    h5file.close()


def update_stock_history(h5file, group, symbol, start_date, end_date):
    table = get_table(h5file, '/history/' + symbol)
    if table is None:
        table = h5file.create_table(group, norm_node_path(symbol), StockHistory, symbol)
    else:
        start_date = parse_date(table[table.nrows - 1]['date'])
        start_date += timedelta(days=1)

    print 'downloading %s from %s to %s' % (symbol, str(start_date)[:10], str(end_date)[:10])
    start_download = time.time()
    serie = list(reversed(download_quote(symbol, start_date, end_date)))
    duration = time.time() - start_download
    print 'download took %s (%d rows)' % (duration, len(serie))

    if len(serie) > 0:
        table.append(np.rec.array(serie))
        table.flush()

    table.close()


def get_table(db, node_path):
    node_path = norm_node_path(node_path)
    return db.get_node(node_path) if node_path in db else None


def norm_node_path(node_path):
    return node_path.replace('^', 'INDEX_')


def parse_date(d):
    try:
        return datetime.strptime(d, '%Y-%m-%d')
    except ValueError:
        return None


def download_quote(symbol, start, end=None):
    '''
    Download the historical prices from Yahoo Finance for the given symbol.

    Arguments:
        str symbol: stock symbol (like AAPL, GOOG...)
        date start: start date of the history to retrieve
        date end: end date of the history to retrieve (default to current date)

    Return:
        list of tuples (date, opening, closing, vol, roi)
    '''
    end = end or datetime.now()

    url = 'http://chart.finance.yahoo.com/table.csv?' + urlencode((
        ('s', symbol),
        ('a', start.month - 1),
        ('b', start.day),
        ('c', start.year),
        ('d', end.month - 1),
        ('e', end.day),
        ('f', end.year),
        ('ignore','.csv'),
    ))

    # TODO: catch urllib2.URLError exception
    # TODO: handle HTTP 5xx and 4xx
    response = urlopen(url)
    reader = csv.reader(response, delimiter=',')

    # skip header
    reader.next()
    output = []

    for date, opening, high, low, closing, vol, adj_closing in reader:
        opening = float(opening)
        closing = float(closing)
        vol = float(vol)
        # TODO: is this the right ROI formula?
        roi = closing / opening - 1
        output.append((date, opening, closing, vol, roi))

    return output


def compute_running_betas(stock, bench, size=30):
    A = np.ones((size, 2), dtype=float64)

    for date, stock_arr, bench_arr in iter_rolling_window(stock, bench, size=30):
        A[:,0] = bench_arr
        # try scipy.stats.linregress
        result = np.linalg.lstsq(A, stock_arr)
        beta, alpha = result[0]
        print date, beta


def iter_rolling_window(stock, bench, size=30):
    # TODO: handle missing data point in stock
    date = bench[size-1]['date']
    stock_arr = np.array([stock[i]['roi'] for i in xrange(size)])
    bench_arr = np.array([bench[i]['roi'] for i in xrange(size)])
    yield date, stock_arr, bench_arr

    n = len(stock)
    for i in xrange(size, n):
        date = bench[i]['date']
        stock_arr[i % size] = stock[i]['roi']
        bench_arr[i % size] = bench[i]['roi']
        yield date, stock_arr, bench_arr


parser = OptionParser(usage=(
    'usage: %prog [options]\n\n'
    'Download historical prices from Yahoo Finance and compute some financial\n'
    'indicators like stock Beta. The first ETL run will create a PyTables database.\n'
    'Subsequent runs will only download the historical data created between the previous\n'
    'run and the current date date (or `end_date` if specified).'
))

parser.add_option('--db-path',
                  dest='db_path',
                  default='data/citadel.h5',
                  help='Path to the PyTables file')

parser.add_option('--start-date',
                  dest='start_date',
                  default='2010-01-01',
                  help='Download history as of this date (yyyy-mm-dd). It has no effect '
                        'if the database already exists')

parser.add_option('--end-date',
                  dest='end_date',
                  help='Download history up to this date (yyyy-mm-dd). Default to the current date')

parser.add_option('--destroy',
                  dest='destroy',
                  action='store_true',
                  help='Destroy the PyTables file which forces all the stocks to be downloaded since '
                       'the `start_date`')


if __name__ == '__main__':
    options, args = parser.parse_args()

    db_dir = os.path.dirname(options.db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    elif options.destroy:
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
    options.end_date = end_date or datetime.now()
    main(options)

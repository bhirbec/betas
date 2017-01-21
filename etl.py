import csv
import time
from datetime import datetime
from optparse import OptionParser
from urllib import urlencode
from urllib2 import urlopen

import numpy as np
from numpy import float64
from tables import open_file, IsDescription, StringCol, Float64Col


NASDAQ = '^IXIC'
APPLE = 'AAPL'


# TODO: use a dict instead
class StockHistory(IsDescription):
    date    = StringCol(10, pos=1)
    opening = Float64Col(pos=2)
    closing = Float64Col(pos=3)
    volume  = Float64Col(pos=4)
    roi     = Float64Col(pos=5)


def main(options):
    start = time.time()

    start_download = time.time()
    stock = list(reversed(download_quote(APPLE, options.start_date)))
    bench = list(reversed(download_quote(NASDAQ, options.start_date)))
    print 'download took %s' % (time.time() - start_download)

    # TODO: create data dir
    h5file = open_file(options.db_path, mode = "w")
    group = h5file.create_group("/", 'history', 'Historical stock prices')

    for symbol, serie in [(NASDAQ, bench), (APPLE, stock)]:
        print 'creating table', symbol
        table = h5file.create_table(group, norm_node_path(symbol), StockHistory, symbol)
        rows = np.rec.array(serie)
        # TODO: write are not thread safe. Need to serialize writes.
        table.append(rows)
        table.flush()
        table.close()

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


def get_table(db, node_path):
    node_path = norm_node_path(node_path)
    return db.get_node(node_path) if node_path in db else None


def norm_node_path(node_path):
    return node_path.replace('^', 'INDEX_')


def max_date(db, node_path):
    tbl = get_table(db, node_path)
    return tbl[tbl.nrows - 1]['date'] if tbl else None


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


USAGE = (
    'usage: %prog [options]\n\n'
    'Download historical prices from Yahoo Finance and compute some financial '
    'indicators like stock Beta.'
)

parser = OptionParser(usage=USAGE)
parser.add_option('-d', '--db-path', dest='db_path',
                  default='data/citadel.h5', help='Path to the PyTables file')
parser.add_option('-s', '--start-date', dest='start_date',
                  default='2010-01-01', help='Download history as of this date (yyyy-mm-dd)')


if __name__ == '__main__':
    options, args = parser.parse_args()

    try:
        options.start_date = datetime.strptime(options.start_date, '%Y-%m-%d')
        main(options)
    except ValueError:
        parser.print_help()

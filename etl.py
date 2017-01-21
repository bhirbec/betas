import csv
import time
from optparse import OptionParser
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
    stock = list(reversed(download_quote(APPLE)))
    bench = list(reversed(download_quote(NASDAQ)))
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


def download_quote(symbol):
    '''
    Returns the historical prices for the given symbol. Data are fetched from
    the Yahoo Finance API.
    '''
    # TODO: use request pkg (http://stackoverflow.com/questions/35371043/use-python-requests-to-download-csv)?
    # TODO: add parameters to specify the time range?
    # TODO: is this the right ROI formula?
    # TODO: handle missing data?
    # TODO: catch urllib2.URLError exception

    # date are inclusive and months start at 0
    base_url = 'http://chart.finance.yahoo.com/table.csv?s={symbol}&a=1&b=1&c=2010&d=0&e=18&f=2017&g=d&ignore=.csv'
    response = urlopen(base_url.format(symbol=symbol))
    reader = csv.reader(response, delimiter=',')

    # skip header
    reader.next()
    output = []

    for date, opening, high, low, closing, vol, adj_closing in reader:
        opening = float(opening)
        closing = float(closing)
        vol = float(vol)
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


parser = OptionParser()
parser.add_option("-d", "--db-path", dest="db_path",
                  default='data/citadel.h5', help="Path to the PyTables file")

if __name__ == '__main__':
    options, args = parser.parse_args()
    main(options)

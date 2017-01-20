import csv
import time
from urllib2 import urlopen

import numpy as np
from numpy import float64


NASDAQ = '^IXIC'
APPLE = 'AAPL'


def main():
    start = time.time()

    start_download = time.time()
    stock = list(reversed(download_quote(APPLE)))
    bench = list(reversed(download_quote(NASDAQ)))
    print 'download took %s' % (time.time() - start_download)

    # These two series are identical in terms of time frame, number of days! This
    # assumption may not hold with other stocks.
    start_beta = time.time()
    compute_running_betas(stock, bench)
    print 'beta computation took %s' % (time.time() - start_beta)

    print 'Total work took %s' % (time.time() - start)


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
    base_url = 'http://chart.finance.yahoo.com/table.csv?s={symbol}&a=1&b=1&c=2010&d=0&e=18&f=2017&g=d&ignore=.csv'
    response = urlopen(base_url.format(symbol=symbol))
    reader = csv.reader(response, delimiter=',')

    # skip header (Date, Open, High, Low, Close, Volume, Adj Close)
    reader.next()

    output = []
    for row in reader:
        date = row[0]
        open_quote = float(row[1])
        close_quote = float(row[4])
        roi = close_quote / open_quote - 1
        output.append((date, roi))
    return output


def compute_running_betas(stock, bench, size=30):
    A = np.ones((size, 2), dtype=float64)

    for date, stock_arr, bench_arr in iter_rolling_window(stock, bench, size=30):
        A[:,0] = bench_arr
        # try scipy.stats.linregress
        result = np.linalg.lstsq(A, stock_arr)
        beta, alpha = result[0]


def iter_rolling_window(stock, bench, size=30):
    date = bench[size-1][0]
    stock_arr = np.array([stock[i][1] for i in xrange(size)])
    bench_arr = np.array([bench[i][1] for i in xrange(size)])
    yield date, stock_arr, bench_arr

    n = len(stock)
    for i in xrange(size, n):
        date = bench[i][0]
        stock_arr[i % size] = stock[i][1]
        bench_arr[i % size] = bench[i][1]
        yield date, stock_arr, bench_arr

if __name__ == '__main__':
    main()

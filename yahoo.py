import csv
import urllib2
from datetime import datetime
from urllib import urlencode

from eventlet import GreenPool
from eventlet.green import urllib2 as gurllib2


# TODO: catch urllib2.URLError exception
# TODO: handle HTTP 5xx and 4xx?


def async_downloads(symbols, pool_size=25):
    '''
    Asynchronously download the historical prices from Yahoo Finance for the given
    list of symbols. Each symbol is represented as a tuple (symbol, start_date,
    end_date).

    Arguments:
        list symbols: list of symbols to download
        int pool_size: green-thread pool size

    Return:
        iterator of 2-uple (symbol, serie). Serie is a list of tuples
        (date, opening, closing, vol, roi).
    '''
    pool = GreenPool(size=pool_size)
    for symbol, resp in pool.imap(_async_download, symbols):
        if resp is not None:
            yield symbol, _parse_response(resp)


def download(symbol, start, end=None):
    '''
    Download the historical prices from Yahoo Finance for the given symbol.

    Arguments:
        str symbol: stock symbol (like AAPL, GOOG...)
        date start: start date of the history to retrieve
        date end: end date of the history to retrieve (default to current date)

    Return:
        list of tuples (date, opening, closing, vol, roi).
    '''
    url = _build_url(symbol, start, end)
    resp = urllib2.urlopen(url)
    return _parse_response(resp)


def _async_download(args):
    symbol, start, end = args
    url = _build_url(symbol, start, end)
    print 'Downloading %s' % symbol
    try:
        return symbol, gurllib2.urlopen(url)
    except gurllib2.HTTPError as e:
        print 'Error: cannot download for symbol %s - %s' % (symbol, e)
        return symbol, None


def _build_url(symbol, start, end=None):
    end = end or datetime.now()
    return 'http://chart.finance.yahoo.com/table.csv?' + urlencode((
        ('s', symbol),
        ('a', start.month - 1),
        ('b', start.day),
        ('c', start.year),
        ('d', end.month - 1),
        ('e', end.day),
        ('f', end.year),
        ('ignore','.csv'),
    ))


def _parse_response(response):
    reader = csv.reader(response, delimiter=',')
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

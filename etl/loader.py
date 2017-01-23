import os
import csv
from datetime import timedelta

import numpy as np
from tables import IsDescription, StringCol, Float64Col
from tables.nodes import filenode

import yahoo
from dblib import get_table, update_table, parse_date


NASDAQ = '^IXIC'
PROJECT_DIR = os.path.dirname(os.path.realpath(__file__))
NASDAQ_FILE = os.path.join(PROJECT_DIR, '../data/nasdaq.csv')
MARKET = [(NASDAQ_FILE, NASDAQ)]


class StockHistory(IsDescription):
    date    = StringCol(10, pos=1)
    opening = Float64Col(pos=2)
    closing = Float64Col(pos=3)
    volume  = Float64Col(pos=4)
    roi     = Float64Col(pos=5)


def load_data(h5file, options):
    if '/stock' not in h5file:
        h5file.create_group("/", 'stock', 'Market components with stocks descriptions')
        for file_path, market_symbol in MARKET:
            print 'Importing %s' % file_path
            _load_stock_list(h5file, file_path, market_symbol)

    if '/history' not in h5file:
        h5file.create_group("/", 'history', 'Historical stock prices')

    if not options.no_download:
        _load_stocks_histories(h5file, options)


def _load_stock_list(h5file, file_path, market_symbol):
    for attrs in _read_stock_list(file_path):
        fnode = filenode.new_node(h5file, where='/stock', name=attrs['symbol'])
        fnode.attrs.content_type = 'text/plain; charset=us-ascii'
        fnode.attrs.market_symbol = market_symbol
        for key, value in attrs.items():
            fnode.attrs[key] = attrs[key]
        fnode.close()

    h5file.flush()


def _read_stock_list(file):
    serie = []
    with open(file, 'r') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        reader.next()

        for symbol, name, _, cap, _, _, sector, industry, _, _ in reader:
            serie.append(dict(
                symbol = symbol,
                name = name.strip(),
                cap = cap,
                sector = sector.strip(),
                industry = industry.strip(),
            ))

    return serie[:100]


def _load_stocks_histories(h5file, options):
    market_symbols = set([])
    symbols = []

    for n in h5file.root.stock:
        symbols.append(n.name)
        market_symbols.add(n.attrs.market_symbol)

    args = []
    symbols += list(market_symbols)
    for symbol in symbols:
        start = _get_symbol_max_date(h5file, symbol) or options.start_date
        args.append((symbol, start, options.end_date))

    for symbol, serie in yahoo.async_downloads(args, pool_size=options.nb_greenthreads):
        serie = list(reversed(serie))
        update_table(h5file, 'history', symbol, serie, StockHistory)


def _get_symbol_max_date(h5file, symbol):
    table = get_table(h5file, '/history/' + symbol)
    if table is None or table.nrows == 0:
        return None

    start_date = parse_date(table[table.nrows - 1]['date'])
    start_date += timedelta(days=1)
    table.close()
    return start_date

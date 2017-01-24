import os
import csv
from datetime import timedelta

import numpy as np
from tables import IsDescription, StringCol, Float64Col

import yahoo
from dblib import get_table, update_table, parse_date, create_json_node, read_json_node


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
    if '/history' not in h5file:
        h5file.create_group("/", 'history', 'Historical stock prices')

    for file_path, market_symbol in MARKET:
        print 'Importing %s' % file_path
        _load_stock_list(h5file, file_path, market_symbol)
        if not options.no_download:
            _load_stocks_histories(h5file, options, market_symbol)


def _load_stock_list(h5file, file_path, market_symbol):
    stocks = _read_stock_list(file_path)
    for s in stocks:
        s['market_symbol'] = market_symbol
    fnode = create_json_node(h5file, dir_name='stock', node_name=market_symbol, content=stocks)


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

    return serie


def _load_stocks_histories(h5file, options, market_symbol):
    market_symbols = set([])
    symbols = []

    for n in read_json_node(h5file, 'stock', market_symbol):
        symbols.append(n['symbol'])
        market_symbols.add(n['market_symbol'])

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

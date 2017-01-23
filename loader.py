import os
import csv
from datetime import timedelta

import numpy as np
from tables import IsDescription, StringCol, Float64Col

import yahoo
from dblib import get_table, update_table, parse_date

NASDAQ = '^IXIC'
PROJECT_DIR = os.path.dirname(os.path.realpath(__file__))
NASDAQ_FILE = os.path.join(PROJECT_DIR, 'data/nasdaq.csv')


class StockDescription(IsDescription):
    Symbol       = StringCol(12, pos=1)
    Name         = StringCol(100, pos=2)
    MarketCap    = Float64Col(pos=3)
    MarketSymbol = StringCol(12, pos=4)
    Sector       = StringCol(100, pos=5)
    Industry     = StringCol(100, pos=6)


class StockHistory(IsDescription):
    date    = StringCol(10, pos=1)
    opening = Float64Col(pos=2)
    closing = Float64Col(pos=3)
    volume  = Float64Col(pos=4)
    roi     = Float64Col(pos=5)


def load_data(h5file, options):
    if '/history' not in h5file:
        h5file.create_group("/", 'history', 'Historical stock prices')

    if '/stock' in h5file:
        h5file.remove_node('/stock', recursive=True)
    h5file.create_group("/", 'stock', 'Market components with stocks descriptions')

    serie = _read_market_components(NASDAQ_FILE, NASDAQ)
    update_table(h5file, 'stock', 'description', serie, StockDescription)

    if not options.no_download:
        _load_stocks_histories(h5file, options)


def _read_market_components(file, market_symbol):
    serie = []
    with open(file, 'r') as f:
        reader = csv.reader(f, delimiter=',', quotechar='"')
        reader.next()

        for row in reader:
            symbol, name, _, cap, _, _, sector, industry, _, _ = [v.strip() for v in row]

            # TODO: remove use of ljust (StringCol are fixed size) :/
            serie.append([
                symbol.ljust(12, ' '),
                name.ljust(100, ' '),
                float(cap),
                market_symbol.ljust(12, ' '),
                sector.ljust(100, ' '),
                industry.ljust(100, ' ')
            ])
    return serie


def _load_stocks_histories(h5file, options):
    start = _get_symbol_max_date(h5file, NASDAQ) or options.start_date
    symbols = [(NASDAQ, start, options.end_date)]

    for n in h5file.root.stock.description:
        symbol = n['Symbol'].strip()
        start = _get_symbol_max_date(h5file, symbol) or options.start_date
        symbols.append((symbol, start, options.end_date))

    for symbol, serie in yahoo.async_downloads(symbols, pool_size=options.nb_greenthreads):
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

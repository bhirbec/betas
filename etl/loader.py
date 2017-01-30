import os
import csv
from datetime import timedelta

import numpy as np
from tables import IsDescription, StringCol, Float64Col

import yahoo
from dblib import parse_date


class StockHistory(IsDescription):
    date    = StringCol(10, pos=1)
    opening = Float64Col(pos=2)
    closing = Float64Col(pos=3)
    volume  = Float64Col(pos=4)
    roi     = Float64Col(pos=5)


def load_data(store, markets, options):
    store.create_dir('/history')

    for file_path, market_symbol in markets:
        print 'Importing %s' % file_path
        stocks = _read_stock_list(file_path)
        n = options.nb_stocks or len(stocks)
        store.put_json('/{0}/stocks'.format(market_symbol), stocks[:n])
        if not options.no_download:
            _load_stocks_histories(store, options, market_symbol)


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


def _load_stocks_histories(store, options, market_symbol):
    stocks = store.get_json('/{0}/stocks'.format(market_symbol))
    symbols = [market_symbol] + [s['symbol'] for s in stocks]

    tasks = []
    for symbol in symbols:
        path = '/{0}/history/{1}'.format(market_symbol, symbol)
        start = _get_symbol_max_date(store, path) or options.start_date
        tasks.append((symbol, start, options.end_date))

    for symbol, serie in yahoo.async_downloads(tasks, pool_size=options.nb_greenthreads):
        serie = list(reversed(serie))
        path = '/{0}/history/{1}'.format(market_symbol, symbol)
        store.update_table(path, serie, StockHistory)


def _get_symbol_max_date(store, path):
    table = store.get_node(path)
    if table is None or table.nrows == 0:
        return None

    start_date = parse_date(table[table.nrows - 1]['date'])
    start_date += timedelta(days=1)
    table.close()
    return start_date

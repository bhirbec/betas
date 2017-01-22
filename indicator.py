import time

import numpy as np
from numpy import float64
from tables import IsDescription, StringCol, Float64Col

from dblib import get_table, update_table


class StockBeta(IsDescription):
    date = StringCol(10, pos=1)
    beta = Float64Col(pos=2)


def compute_indicators(h5file, bench_symbol, window_size=30):
    if '/indicator' in h5file:
        h5file.remove_node('/indicator', recursive=True)
    h5file.create_group("/", 'indicator', 'Stock Betas over time')

    table = get_table(h5file, '/history/' + bench_symbol)
    if table is None:
        print 'Error: benchmark table is empty'
        return

    bench = table.read()
    table.close()

    for n in h5file.root.history:
        symbol = n.name
        stock = n.read()
        n.close()

        if len(stock) > window_size:
            start_time = time.time()
            stock_serie, bench_serie = _join_series(stock, bench, 'date')
            betas = _compute_running_betas(stock_serie, bench_serie, window_size)
            update_table(h5file, 'indicator', symbol, betas, StockBeta)
            print 'Computing Betas for %s took %s' % (symbol, (time.time() - start_time))


def _compute_running_betas(stock, bench, window_size=30):
    A = np.ones((window_size, 2), dtype=float64)

    output = []
    for date, stock_arr, bench_arr in _iter_window(stock, bench, window_size):
        A[:,0] = bench_arr
        # try scipy.stats.linregress
        result = np.linalg.lstsq(A, stock_arr)
        beta, alpha = result[0]
        output.append((date, beta))

    return output


def _join_series(s1, s2, key):
    out1, out2 = [], []
    s1, s2 = iter(s1), iter(s2)
    row1 = next(s1, None)
    row2 = next(s2, None)

    while row1 and row2:
        key1, key2 = row1[key], row2[key]
        if key1 > key2:
            row2 = next(s2, None)
        elif key1 < key2:
            row1 = next(s1, None)
        else:
            out1.append(row1)
            out2.append(row2)
            row2 = next(s2, None)
            row1 = next(s1, None)

    return out1, out2


def _iter_window(stock, bench, size=30):
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

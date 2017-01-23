import time
import traceback
from multiprocessing import Pool

import numpy as np
from numpy import float64
from tables import IsDescription, StringCol, Float64Col

from dblib import get_table, update_table


WINDOW_SIZE = 30


class StockBeta(IsDescription):
    date = StringCol(10, pos=1)
    beta = Float64Col(pos=2)


def compute_indicators(h5file, bench_symbol, nb_proc=2):
    if '/indicator' in h5file:
        h5file.remove_node('/indicator', recursive=True)
    h5file.create_group("/", 'indicator', 'Stock Betas over time')

    table = get_table(h5file, '/history/' + bench_symbol)
    if table is None:
        print 'Error: benchmark table is empty'
        return

    bench = table.read()
    table.close()

    pool = Pool(processes=nb_proc)
    for result in pool.imap_unordered(_work, _iter_series(h5file, bench)):
        if result is None:
            continue

        symbol, betas = result
        update_table(h5file, 'indicator', symbol, betas, StockBeta)


def _iter_series(h5file, bench):
    for n in h5file.root.history:
        yield n.name, n.read(), bench
        n.close()


def _work(args):
    try:
        return _running_betas(*args)
    except Exception as e:
       print traceback.format_exc()
       return None


def _running_betas(symbol, stock, bench):
    start_time = time.time()

    if len(stock) <= WINDOW_SIZE:
        return symbol, []

    stock_serie, bench_serie = _join_series(stock, bench, 'date')
    A = np.ones((WINDOW_SIZE, 2), dtype=float64)

    betas = []
    for date, stock_win, bench_win in _iter_window(stock_serie, bench_serie, WINDOW_SIZE):
        A[:,0] = bench_win
        # try scipy.stats.linregress
        result = np.linalg.lstsq(A, stock_win)
        beta, alpha = result[0]
        betas.append((date, beta))

    print 'Computing Betas for %s took %s' % (symbol, (time.time() - start_time))
    return symbol, betas


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
    stock_win = np.array([stock[i]['roi'] for i in xrange(size)])
    bench_win = np.array([bench[i]['roi'] for i in xrange(size)])
    yield date, stock_win, bench_win

    n = len(stock)
    for i in xrange(size, n):
        date = bench[i]['date']
        stock_win[i % size] = stock[i]['roi']
        bench_win[i % size] = bench[i]['roi']
        yield date, stock_win, bench_win

import time
import traceback
from multiprocessing import Pool

import numpy as np
from numpy import float64
from tables import IsDescription, StringCol, Float64Col

from dblib import inner_join


WINDOW_SIZE = 30


class StockBeta(IsDescription):
    date = StringCol(10, pos=1)
    beta = Float64Col(pos=2)


def compute_indicators(store, markets, nb_proc=2):
    for _, bench_symbol in markets:
        path = '/{0}/indicator'.format(bench_symbol)
        store.remove_path(path)

        pool = Pool(processes=nb_proc)
        for result in pool.imap_unordered(_work, _iter_tasks(store, bench_symbol)):
            if result is not None:
                symbol, betas = result
                path = '/{0}/indicator/{1}'.format(bench_symbol, symbol)
                store.update_table(path, betas, StockBeta)


def _iter_tasks(store, bench_symbol):
    path = '/{0}/history/{1}'.format(bench_symbol, bench_symbol)
    bench = store.get_node(path)
    if bench is None:
        print 'Error: benchmark table is empty'
        return

    bench_data = bench.read()
    bench.close()

    for n in store.get_node('/{0}/history'.format(bench_symbol)):
        data = n.read()
        if len(data) >= WINDOW_SIZE:
            yield n.name, data, bench_data
        n.close()


def _work(args):
    try:
        start_time = time.time()
        symbol, stock, bench = args
        stock_serie, bench_serie = inner_join(stock, bench, 'date')

        # TODO: do we need float64?
        dates = [b['date'] for b in bench_serie][WINDOW_SIZE:]
        stock_serie = np.array([v['roi'] for v in stock_serie], dtype=float64)
        bench_serie = np.array([v['roi'] for v in bench_serie], dtype=float64)
        betas = _running_betas(bench_serie, stock_serie, WINDOW_SIZE)
        print 'Computing Betas for %s took %s' % (symbol, (time.time() - start_time))
        # TODO: let's try to remove that zip
        return symbol, zip(dates, betas)
    except Exception as e:
       print traceback.format_exc()

    return None


def _running_betas(x, y, size):
    x_wins = _rolling_window(x, size)
    x_means = _rolling_means(x, size)[size-1:, None]

    y_wins = _rolling_window(y, size)
    y_means = _rolling_means(y, size)[size-1:, None]

    numerator = np.sum((x_wins - x_means) * (y_wins - y_means), axis=1)
    denominator = np.sum((x_wins - x_means)**2, axis=1)
    return numerator / denominator


def _rolling_window(a, size):
    shape = a.shape[:-1] + (a.shape[-1] - size + 1, size)
    strides = a.strides + (a.strides[-1],)
    return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)


def _rolling_means(x, size):
    cumul = np.cumsum(x)
    cumul[size:] = cumul[size:] - cumul[:-size]
    return cumul / size

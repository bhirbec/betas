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
        betas = _running_betas(bench_serie, stock_serie, WINDOW_SIZE)
        print 'Computing Betas for %s took %s' % (symbol, (time.time() - start_time))
        return symbol, betas
    except Exception as e:
       print traceback.format_exc()

    return None


def _running_betas(x, y, size):
    output = []
    iter_wins = _iter_windows(x, y, 'roi', size)
    x_win, y_win = iter_wins.next()

    x_sum = np.sum(x_win)
    y_sum = np.sum(y_win)
    x_mean = x_sum / size
    y_mean = y_sum / size
    numerator = np.sum( (x_win - x_mean) * (y_win - y_mean) )
    denominator = np.sum((x_win - x_mean)**2)
    output.append((x[size]['date'], numerator / denominator))

    for i, (x_win, y_win) in enumerate(iter_wins, size):
        x_sum += x[i]['roi'] - x[i-size]['roi']
        y_sum += y[i]['roi'] - y[i-size]['roi']
        x_mean = x_sum / size
        y_mean = y_sum / size
        numerator = np.sum( (x_win - x_mean) * (y_win - y_mean) )
        denominator = np.sum((x_win - x_mean)**2)
        output.append((x[i]['date'], numerator / denominator))

    return output


def _iter_windows(x, y, attr, size):
    x_win = np.array([x[i][attr] for i in xrange(size)])
    y_win = np.array([y[i][attr] for i in xrange(size)])
    yield x_win, y_win

    n = len(x)
    for i in xrange(size, n):
        x_win[i % size] = x[i][attr]
        y_win[i % size] = y[i][attr]
        yield x_win, y_win

import time
import traceback
from multiprocessing import Pool

import numpy as np
from numpy import float64
from tables import IsDescription, StringCol, Float64Col


WINDOW_SIZE = 30


class StockBeta(IsDescription):
    date = StringCol(10, pos=1)
    beta = Float64Col(pos=2)
    beta2 = Float64Col(pos=3)
    # beta3 = Float64Col(pos=4)


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
        yield n.name, n.read(), bench_data
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

    dates, stock_serie, bench_serie = _join_series(stock, bench, 'date', 'roi')
    betas1 = list(beta_py(bench_serie, stock_serie, WINDOW_SIZE))
    betas2 = beta_vec(stock_serie, bench_serie)
    # betas3 = stacked_lstsq(stock_serie, bench_serie)

    print 'Computing Betas for %s took %s' % (symbol, (time.time() - start_time))
    return symbol, zip(dates[WINDOW_SIZE:], betas1, betas2)


def beta_vec(stock_serie, bench_serie):
    A = np.ones((WINDOW_SIZE, 2), dtype=float64)

    def _lstsq(stock_win, bench_win):
        A[:,0] = bench_win
        beta, alpha = np.linalg.lstsq(A, stock_win)[0]
        return np.array([beta])

    def _cov(stock_win, bench_win):
        cov = np.cov(stock_win, bench_win)
        return np.array([cov[0][1] / cov[1][1]])

    def rolling_window(a, window):
        # from http://www.rigtorp.se/2011/01/01/rolling-statistics-numpy.html
        shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
        strides = a.strides + (a.strides[-1],)
        return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)

    stock_wins = rolling_window(stock_serie, WINDOW_SIZE)
    bench_wins = rolling_window(bench_serie, WINDOW_SIZE)
    vfunc = np.vectorize(_lstsq, signature='(n),(m)->(k)')
    return vfunc(stock_wins, bench_wins).flatten()


def beta_py(x, y, size):
    x_win = np.array([x[i] for i in xrange(size)])
    y_win = np.array([y[i] for i in xrange(size)])
    c = np.cov(x_win, y_win)
    yield c[0][1] / c[0][0]

    x_sum = sum(x[i] for i in xrange(size))
    y_sum = sum(y[i] for i in xrange(size))
    x_mean = x_sum / size
    y_mean = y_sum / size

    n = len(x)
    for i in xrange(size, n):
        x_win[i % size] = x[i]
        y_win[i % size] = y[i]

        x_sum += x[i] - x[i-size]
        y_sum += y[i] - y[i-size]
        x_mean = x_sum / size
        y_mean = y_sum / size
        numerator = np.sum( (x_win - x_mean) * (y_win - y_mean) )
        denominator = np.sum((x_win - x_mean)**2)

        yield numerator / denominator


# http://stackoverflow.com/questions/30442377/how-to-solve-many-overdetermined-systems-of-linear-equations-using-vectorized-co
def stacked_lstsq(L, b, rcond=1e-10):
    """
    Solve L x = b, via SVD least squares cutting of small singular values
    L is an array of shape (..., M, N) and b of shape (..., M).
    Returns x of shape (..., N)
    """

    def rolling_window(a, window):
        shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
        strides = a.strides + (a.strides[-1],)
        return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)

    L = rolling_window(L, WINDOW_SIZE)
    b = rolling_window(b, WINDOW_SIZE)

    u, s, v = np.linalg.svd(L, full_matrices=False)
    s_max = s.max(axis=-1, keepdims=True)
    s_min = rcond*s_max
    inv_s = np.zeros_like(s)
    inv_s[s >= s_min] = 1/s[s>=s_min]
    x = np.einsum('...ji,...j->...i', v,
                  inv_s * np.einsum('...ji,...j->...i', u, b.conj()))
    return np.conj(x, x)


def _join_series(s1, s2, key, attr):
    keys, out1, out2 = [], [], []
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
            keys.append(key1)
            out1.append(row1[attr])
            out2.append(row2[attr])
            row2 = next(s2, None)
            row1 = next(s1, None)

    return keys, np.array(out1), np.array(out2)

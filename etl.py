import sys
import os
import time
from datetime import datetime, timedelta
from optparse import OptionParser

import numpy as np
from numpy import float64
from tables import open_file, IsDescription, StringCol, Float64Col


import yahoo


NASDAQ = '^IXIC'

SYMBOLS = [
    'PIH', 'FLWS', 'FCCY', 'SRCE', 'VNET', 'TWOU', 'JOBS', 'CAFD', 'EGHT', 'AVHI', 'SHLM', 'AAON', 'ABAX', 'ABEO',
    'ABEOW', 'ABIL', 'ABMD', 'AXAS', 'ACIU', 'ACIA', 'ACTG', 'ACHC', 'ACAD', 'ACST', 'AXDX', 'XLRN', 'ANCX', 'ARAY',
    'ACRX', 'ACET', 'AKAO', 'ACHN', 'ACIW', 'ACRS', 'ACNB', 'ACOR', 'ATVI', 'ACTA', 'ACUR', 'ACXM', 'ADMS', 'ADMP',
    'ADAP', 'ADUS', 'AEY', 'IOTS', 'ADMA', 'ADBE', 'ADTN', 'ADRO', 'AAAP', 'ADES', 'AEIS', 'AMD', 'ADXS', 'ADXSW',
    'ADVM', 'MAUI', 'AEGN', 'AGLE', 'AEHR', 'AMTX', 'AEPI', 'AERI', 'AVAV', 'AEZS', 'AEMD', 'GNMX', 'AFMD', 'AGEN',
    'AGRX', 'AGYS', 'AGIO', 'AGNC', 'AGNCB', 'AGNCP', 'AGFS', 'AGFSW', 'AIMT', 'AIRM', 'AIRT', 'ATSG', 'AIRG', 'AMCN',
    'AKAM', 'AKTX', 'AKBA', 'AKER', 'AKRX', 'ALRM', 'ALSK', 'AMRI', 'ALBO', 'ABDC', 'ADHD', 'ALDR', 'ALDX', 'ALXN',
    'ALCO', 'ALGN', 'ALIM', 'ALJJ', 'ALKS', 'ABTX', 'ALGT', 'AIQ', 'AHGP', 'AMMA', 'ARLP', 'AHPI', 'AMOT', 'ALQA',
    'ALLT', 'MDRX', 'AFAM', 'ALNY', 'AOSL', 'GOOG', 'GOOGL', 'SMCP', 'ATEC', 'SWIN', 'ASPS', 'AIMC', 'AMAG', 'AMRN',
    'AMRK', 'AYA', 'AMZN', 'AMBC', 'AMBCW', 'AMBA', 'AMCX', 'DOX', 'AMDA', 'AMED', 'UHAL', 'ATAX', 'AMOV', 'AAL',
    'ACSF', 'AETI', 'AMNB', 'ANAT', 'AOBC', 'APEI', 'ARII', 'AMRB', 'AMSWA', 'AMSC', 'AMWD', 'CRMT', 'ABCB', 'AMSF',
    'ASRV', 'ASRVP', 'ATLO', 'AMGN', 'FOLD', 'AMKR', 'AMPH', 'IBUY', 'ASYS', 'AFSI', 'AMRS', 'ADI', 'ALOG', 'AVXL',
    'ANCB', 'ANDA', 'ANDAR', 'ANDAU', 'ANDAW', 'ANGI', 'ANGO', 'ANIP', 'ANIK', 'ANSS', 'ATRS', 'ANTH', 'ABAC', 'APOG',
    'APOL', 'APEN', 'AINV', 'APPF', 'AAPL', 'ARCI', 'APDN', 'APDNW', 'AGTC', 'AMAT', 'AMCC', 'AAOI', 'AREX', 'APTI',
    'APRI', 'APVO', 'APTO', 'AQMS', 'AQB', 'AQXP', 'ARDM', 'ARLZ', 'PETX', 'ABUS', 'ARCW', 'ABIO', 'RKDA', 'ARCB',
    'ACGL', 'ACGLP', 'APLP', 'ACAT', 'ARDX', 'ARNA', 'ARCC', 'AGII', 'AGIIL', 'ARGS', 'ARIS', 'ARIA', 'ARKR', 'ARTX',
    'ARQL', 'ARRY', 'ARRS', 'DWAT', 'AROW', 'ARWR', 'ARTNA', 'ARTW', 'ASBB', 'ASNA', 'ASND', 'ASCMA', 'APWC', 'ASML',
    'AZPN', 'ASMB', 'ASFI', 'ASTE', 'ATRO', 'ALOT', 'ASTC', 'ASUR', 'ATAI', 'ATRA', 'ATHN', 'ATHX', 'AAPC', 'AAME',
    'ACBI', 'ACFC', 'ABY', 'ATLC', 'AAWW', 'AFH', 'TEAM', 'ATNI', 'ATOM', 'ATOS', 'ATRC', 'ATRI', 'ATTU', 'LIFE',
    'AUBN', 'BOLD', 'AUDC', 'AUPH', 'EARS', 'ABTL', 'ADSK', 'ADP', 'AVDL', 'AVEO', 'AVXS', 'AVNW', 'AVID', 'AVGR',
    'AVIR', 'CAR', 'AHPA', 'AHPAU', 'AHPAW', 'AWRE', 'AXAR', 'AXARU', 'AXARW', 'ACLS', 'AXGN', 'AXSM', 'AXTI', 'AZRX',
    'BCOM', 'RILY', 'RILYL', 'BOSC', 'BEAV', 'BIDU', 'BCPC', 'BWINA', NASDAQ
]

class StockHistory(IsDescription):
    date    = StringCol(10, pos=1)
    opening = Float64Col(pos=2)
    closing = Float64Col(pos=3)
    volume  = Float64Col(pos=4)
    roi     = Float64Col(pos=5)


class StockBeta(IsDescription):
    date = StringCol(10, pos=1)
    beta = Float64Col(pos=2)


def main(options):
    start_time = time.time()
    mode = 'a' if os.path.exists(options.db_path) else 'w'

    h5file = open_file(options.db_path, mode=mode)
    _update_history(h5file, options)
    _compute_indicators(h5file)

    print 'Total work took %s' % (time.time() - start_time)
    h5file.close()


def _update_history(h5file, options):
    if '/history' not in h5file:
        h5file.create_group("/", 'history', 'Historical stock prices')

    if not options.no_download:
        symbols = []
        for symbol in SYMBOLS:
            start = _get_symbol_max_date(h5file, symbol) or options.start_date
            symbols.append((symbol, start, options.end_date))

        for symbol, serie in yahoo.async_downloads(symbols, pool_size=10):
            _update_history_table(h5file, '/history', symbol, serie)


def _get_symbol_max_date(h5file, symbol):
    table = get_table(h5file, '/history/' + symbol)
    if table is None or table.nrows == 0:
        return None

    start_date = parse_date(table[table.nrows - 1]['date'])
    start_date += timedelta(days=1)
    table.close()
    return start_date


def _update_history_table(h5file, group, symbol, serie):
    print 'Updating table for %s (%d values)' % (symbol, len(serie))
    table = get_table(h5file, '/history/' + symbol)
    if table is None:
        table = h5file.create_table(group, norm_node_path(symbol), StockHistory, symbol)

    if len(serie) > 0:
        serie = list(reversed(serie))
        table.append(np.rec.array(serie))
        table.flush()

    table.close()


def get_table(db, node_path):
    path = norm_node_path(node_path)
    return db.get_node(path) if path in db else None


def norm_node_path(node_path):
    return node_path.replace('^', 'INDEX_')


def parse_date(d):
    try:
        return datetime.strptime(d, '%Y-%m-%d')
    except ValueError:
        return None


def _compute_indicators(h5file, window_size=30):
    if '/indicator' in h5file:
        h5file.remove_node('/indicator', recursive=True)
    h5file.create_group("/", 'indicator', 'Stock Betas over time')

    table = get_table(h5file, '/history/' + NASDAQ)
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
            # These two series are identical in terms of time frame, number of days! This
            # assumption may not hold with other stocks.
            start_time = time.time()

            stock_serie, bench_serie = _join_series(stock, bench, 'date')
            betas = _compute_running_betas(stock_serie, bench_serie, window_size)

            table = h5file.create_table('/indicator', norm_node_path(symbol), StockBeta)
            table.append(np.rec.array(betas))
            table.close()
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


parser = OptionParser(usage=(
    'usage: %prog [options]\n\n'
    'Download historical prices from Yahoo Finance and compute some financial\n'
    'indicators like stock Beta. The first ETL run will create a PyTables database.\n'
    'Subsequent runs will only download the historical data generated between the previous\n'
    'run and the current date (or `end_date` if specified).'
))

parser.add_option('--db-path',
                  dest='db_path',
                  default='data/citadel.h5',
                  help='Path to the PyTables file')

parser.add_option('--start-date',
                  dest='start_date',
                  default='2010-01-01',
                  help='Download history as of this date (yyyy-mm-dd). It has no effect '
                        'if the database already exists')

parser.add_option('--end-date',
                  dest='end_date',
                  help='Download history up to this date (yyyy-mm-dd). Default to the current date')

parser.add_option('--destroy',
                  dest='destroy',
                  action='store_true',
                  help='Destroy the PyTables file which forces all the stocks to be downloaded since '
                       'the `start_date`')

parser.add_option('--no-download',
                  dest='no_download',
                  action='store_true',
                  help='Do not download any data from Yahoo - just compute indicators')


if __name__ == '__main__':
    options, args = parser.parse_args()

    db_dir = os.path.dirname(options.db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    elif options.destroy:
        os.remove(options.db_path)

    start_date = parse_date(options.start_date)
    if start_date is None:
        parser.print_help()
        sys.exit("Wrong parameter `start_end`: %s" % options.start_date)

    end_date = None
    if options.end_date:
        end_date = parse_date(options.end_date)
        if end_date is None:
            parser.print_help()
            sys.exit("Wrong parameter `end_date`: %s" % options.end_date)

    options.start_date = start_date
    options.end_date = end_date or datetime.now()
    main(options)

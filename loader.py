from datetime import timedelta

import numpy as np
from tables import IsDescription, StringCol, Float64Col

import yahoo
from dblib import get_table, update_table, parse_date


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


def load_data(h5file, options):
    if '/history' not in h5file:
        h5file.create_group("/", 'history', 'Historical stock prices')

    if not options.no_download:
        symbols = []
        for symbol in SYMBOLS:
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

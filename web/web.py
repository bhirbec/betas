import time
import os
from optparse import OptionParser

from flask import jsonify
from flask import Flask
from flask import render_template
from flask import request

from etl.dblib import Storage


PROJECT_DIR = os.path.dirname(os.path.realpath(__file__))


app = Flask(__name__)
db_path = os.path.join(PROJECT_DIR, '../db.h5')


def storage(f):
    def _f(*args, **kwargs):
        try:
            store = Storage(db_path)
            return f(store, *args, **kwargs)
        finally:
            store.close()

    _f.__name__ = f.__name__
    return _f


@app.route('/')
def home():
    return render_template('index.html', ts=time.time())


@app.route('/stock_list/<market>')
@storage
def stock_list(store, market):
    q = request.args.get('q')
    limit = request.args.get('l') or 25

    path = '/{0}/stocks'.format(market)
    stocks = store.get_json(path)
    stocks = sorted(stocks, key=lambda s: s['name'])

    output = []
    count = 0
    for s in stocks:
        if not q or q.lower() in s['name'].lower():
            output.append(s)
            count += 1
            if count >= limit:
                break

    return jsonify(output)


@app.route('/stock_betas/<market>/<symbol>')
@storage
def stock_betas(store, market, symbol):
    path = '/{0}/indicator/{1}'.format(market, symbol)
    table = store.get_node(path)
    if table is None:
        return ''

    start = request.args.get('start')
    end = request.args.get('end')

    dates, betas = ['x'], ['Betas (30 days)']
    for r in table.read():
        date = r['date']
        if (not start or start <= date) and (not end or date <= end):
            dates.append(r['date'])
            betas.append(r['beta'])

    return jsonify({'dates': dates, 'betas': betas})


if __name__ == '__main__':
    parser = OptionParser(usage=(
        'usage: python %prog [options]\n\n'
        'Start a web server that provides Financial reports.\n'
    ))

    parser.add_option('--host', dest='host', default='localhost', help='TCP Host (default: localhost)')
    parser.add_option('--port', dest='port', default='8080', help='TCP port (default: 8080)')
    options, _ = parser.parse_args()
    app.run(host=options.host, port=options.port)

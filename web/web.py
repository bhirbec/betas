import time
from optparse import OptionParser

from flask import jsonify
from flask import Flask
from flask import render_template
from flask import request

from etl.dblib import Storage


NASDAQ = '^IXIC'

app = Flask(__name__)


def storage(f):
    def _f(*args, **kwargs):
        try:
            store = Storage(options.db_path)
            return f(store, *args, **kwargs)
        finally:
            store.close()

    _f.__name__ = f.__name__
    return _f


@app.route('/')
def home():
    return render_template('index.html', ts=time.time())


@app.route('/stock_list')
@storage
def stock_list(store):
    stocks = store.get_json('/stock/' + NASDAQ)
    output = sorted(stocks, key=lambda s: s['name'])
    return jsonify(output)


@app.route('/stock_betas/<symbol>')
@storage
def stock_betas(store, symbol):
    table = store.get_node('/indicator/' + symbol)
    if table is None:
        return '[]'

    output = []
    start = request.args.get('start')
    end = request.args.get('end')

    for r in table.read():
        date = r['date']
        if (not start or start <= date) and (not end or date <= end):
            output.append(list(r))

    return jsonify(output)


parser = OptionParser(usage=(
    'usage: python %prog [options]\n\n'
    'Start a web server that provides Financial reports.\n'
))

parser.add_option('--host', dest='host', default='localhost', help='TCP Host (default: localhost)')
parser.add_option('--port', dest='port', default='8080', help='TCP port (default: 8080)')
parser.add_option('--db-path', dest='db_path', default='db.h5', help='Path to the PyTables file')
options, _ = parser.parse_args()

if __name__ == '__main__':
    app.run(host=options.host, port=options.port)

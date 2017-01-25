import time

from flask import jsonify
from flask import Flask
from flask import render_template
from flask import request

from etl.dblib import Storage


NASDAQ = '^IXIC'

app = Flask(__name__)
store = Storage('db.h5')
stocks = store.read_json_node('stock', NASDAQ)

@app.route('/')
def home():
    return render_template('index.html', ts=time.time())

@app.route('/stock_list')
def stock_list():
    return jsonify(stocks)


@app.route('/stock_betas/<symbol>')
def stock_betas(symbol):
    table = store.get_table('/indicator/' + symbol)
    if table is None:
        return '[]'

    output = []
    start = request.args.get('start')
    end = request.args.get('end')

    for r in table.read():
        date = r['date']
        if (not start or start <= date) and (not end or date <= end):
            output.append([date, r['beta']])

    return jsonify(output)

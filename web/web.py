import time

from flask import jsonify
from flask import Flask
from flask import render_template

from etl.dblib import open_db, get_table, read_json_node


NASDAQ = '^IXIC'

app = Flask(__name__)
db = open_db('db.h5')


@app.route('/')
def home():
    return render_template('index.html', ts=time.time())

@app.route('/stock_list')
def stock_list():
    stocks = read_json_node(db, 'stock', NASDAQ)
    return jsonify(stocks)


@app.route('/stock_betas/<symbol>')
def stock_betas(symbol):
    table = get_table(db, '/indicator/' + symbol)
    if table is None:
        return '[]'

    output = []
    for r in table.read():
        output.append([r['date'], r['beta']])
    return jsonify(output)

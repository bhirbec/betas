import time

from flask import jsonify
from flask import Flask
from flask import render_template

from etl.dblib import open_db, get_table


app = Flask(__name__)
db = open_db('db.h5')


@app.route('/')
def home():
    return render_template('index.html', ts=time.time())

# TODO: this is super slow
@app.route('/stock_list')
def stock_list():
    start = time.time()
    stocks = []
    n = 10
    for i, s in enumerate(db.root.stock):
        if i > n:
            break
        tbl = get_table(db, '/history/' + s.attrs.symbol)
        if tbl and tbl.nrows > 0:
            stocks.append(dict(symbol=s.attrs.symbol, name=s.attrs.name))
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

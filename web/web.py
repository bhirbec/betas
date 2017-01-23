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


@app.route('/stock_list')
def stock_list():
	stocks = []
	for s in db.root.stock:
		stocks.append(dict(symbol=s.attrs.symbol, name=s.attrs.name))
	return jsonify(stocks)


@app.route('/stock_betas/<symbol>')
def stock_betas(symbol):
	table = get_table(db, '/indicator/' + symbol)
	if table is None:
		return '[]'

	dates, betas = [], []
	for r in table.read():
		dates.append(r['date'])
		betas.append(r['beta'])
	return jsonify(dict(dates=dates, betas=betas))

(function () {
    function loadApp() {
        ReactDOM.render(<App />, document.getElementById('app'));
    }

    var timeout;
    var query = '';

    var App = React.createClass({
        getInitialState: function () {
            return {}
        },

        componentDidMount: function () {
            var that = this
            $.get('/stock_list', function (stocks) {
                that.setState({origStock: stocks, stocks: stocks, selected: stocks[0] || {}});
            });

            var $n = $(ReactDOM.findDOMNode(this));
            $n.on('keyup', '#stock-search input', function (e) {
                var newQuery = $n.find('#stock-search input').val();
                if (query == newQuery) {
                    return
                } else {
                    query = newQuery;
                }

                clearTimeout(timeout)
                timeout = setTimeout(function() {that.update(query)}, 200);
            });
        },

        update: function (s) {
            var that = this;
            var stocks = [];

            for (var i=0; i < this.state.origStock.length; i++) {
                var stock = this.state.origStock[i];
                if (stock.name.toLowerCase().indexOf(s) != -1) {
                    stocks.push(stock);
                }
            }

            that.setState({stocks: stocks, selected: stocks[0] || {}});
        },

        handleClick: function(stock, e) {
            this.setState({selected: stock})
            e.preventDefault();
        },

        render: function() {
            return <div>
                {'origStock' in this.state ?
                    <div>
                        <StockList stocks={this.state.stocks} selected={this.state.selected} handleClick={this.handleClick} />
                        {'symbol' in this.state.selected ?
                            <Report stock={this.state.selected} />
                            :
                            <div id="result">
                                <h2>No Match...</h2>
                            </div>
                        }

                    </div>
                    :
                    null
                }
            </div>
        }
    });

    var Report = React.createClass({
        getInitialState: function () {
            return {start: '2010-01-01', end: ''}
        },

        shouldComponentUpdate: function (nextProps, nextState) {
            return this.props.stock.symbol != nextProps.stock.symbol
        },

        componentDidMount: function () {
            var that = this;
            var $n = $(ReactDOM.findDOMNode(this));

            $n.find('.datepicker').datepicker({
                format: "yyyy-mm-dd",
                autoclose: true
            }).on('changeDate', function () {
                that.setState({
                    start: $('#start-date').val(),
                    end: $('#end-date').val(),
                })
                that.update(that.props.stock, that.state);
            })
        },

        componentWillUpdate: function (nextProps, nextState) {
            this.update(nextProps.stock, nextState)
        },

        update: function(stock, state) {
            $.get('/stock_betas/' + stock.symbol, state, function (data) {
                drawGoogleChart(stock, data);
            });
        },

        render: function () {
            return <div id="result">
                <h2>{this.props.stock.name} ({this.props.stock.symbol})</h2>
                <div id="dates-form">
                    <label>From: </label>
                    <input id="start-date" type="text" className={"datepicker"} />
                    <label>To: </label>
                    <input id="end-date" type="text" className={"datepicker"} />
                </div>
                <div id="chart"></div>
            </div>
        }
    });

    var StockList = React.createClass({
        render: function() {
            var that = this;
            var text = "Search over " + this.props.stocks.length + " stocks..."
            return <div id="left-nav">
                    <div id='stock-search'>
                        <input type="text" className={"form-control"} placeholder={text} />
                    </div>
                    <div id='stock-list'>
                        {this.props.stocks.map(function (s) {
                            var klass = s.symbol == that.props.selected.symbol ? 'selected' : '';
                            return <a key={s.symbol} href={'#' + s.symbol} onClick={that.props.handleClick.bind(that, s)} className={klass}>
                                {s.name} ({s.symbol})
                            </a>
                        })}
                    </div>
            </div>
        }
    });

    function drawGoogleChart(stock, rows) {
        var data = new google.visualization.DataTable();
        data.addColumn('date', 'Time');
        data.addColumn('number', 'Beta');

        for (var i = 0; i < rows.length; i++) {
            var parts = rows[i][0].split('-')
            rows[i][0] = new Date(parts[0], parts[1], parts[2])
        }

        data.addRows(rows);

        var options = {
            chart: {
              title: 'Beta Over Time - ' + stock.name,
            },
            width: 900,
            height: 400
        };

        var chart = new google.charts.Line(document.getElementById('chart'));
        chart.draw(data, options);
    }

    google.charts.load('current', {'packages':['line']});
    google.charts.setOnLoadCallback(loadApp);
})()


(function () {

    var timeout;
    var query = '';

    var MARKETS = {
        '^IXIC': 'NASDAQ',
        '^NYA': 'NYSE',
    }

    var App = React.createClass({
        getInitialState: function () {
            return {'market': '^IXIC'}
        },

        handleClick: function (symbol, e) {
            this.setState({market: symbol});
            e.preventDefault();
        },

        render: function() {
            return <div id={"app"}>
                <div id={"header"}>
                    <a href={"#"}
                       onClick={this.handleClick.bind(this, '^IXIC')}
                       className={'^IXIC' == this.state.market ? 'selected' : ''}>
                       NASDAQ
                    </a>
                    <a href={"#"}
                       onClick={this.handleClick.bind(this, '^NYA')}
                       className={'^NYA' == this.state.market ? 'selected' : ''}>
                       NYSE
                    </a>
                </div>
                <Content market={this.state.market}></Content>
            </div>
        }
    });

    var Content = React.createClass({
        getInitialState: function () {
            return {stocks: [], selected: {}, index: {}}
        },

        componentDidMount: function() {
            this.update('')
        },

        componentWillReceiveProps: function (nextProps) {
            this.update('', nextProps.market)
        },

        update: function (q, market) {
            var that = this;
            var market =  market || this.props.market;

            $.get('/stock_list/' + market, {q: q}, function (stocks) {
                var index = {}
                for (var i = 0; i < stocks.length; i++) {
                    index[stocks[i].symbol] = stocks[i];
                }
                that.setState({stocks: stocks, selected: stocks[0] || {}, index: index});
            });
        },

        handleKeyUp: function (e) {
            var $n = $(ReactDOM.findDOMNode(this));
            var $selected = $n.find('#stock-list a.selected');
            var symbol = '';

            if (e.keyCode == 40) {
                symbol = ($selected.next().attr('href') || '').substring(1);
            } else if (e.keyCode == 38) {
                symbol = ($selected.prev().attr('href') || '').substring(1);
            }

            if (symbol != '') {
                this.setState({selected: this.state.index[symbol]});
            }

            var newQuery = $('#stock-search input').val();
            if (query == newQuery) {
                return
            } else {
                query = newQuery;
            }

            var that = this
            clearTimeout(timeout)
            timeout = setTimeout(function() {that.update(query)}, 200);
        },

        handleClick: function(stock, e) {
            this.setState({selected: stock})
            e.preventDefault();
        },

        render: function() {
            return <div id={"content"}>
                <div className={"scroll"}>
                    <StockList stocks={this.state.stocks}
                               market={this.props.market}
                               selected={this.state.selected} handleClick={this.handleClick}
                               handleKeyUp={this.handleKeyUp} />
                    {'symbol' in this.state.selected ?
                        <Report stock={this.state.selected} market={this.props.market} />
                        :
                        <div id="result">
                            <h2>No Match...</h2>
                        </div>
                    }
                </div>
            </div>
        }
    });

    var Report = React.createClass({
        getInitialState: function () {
            return {start: '2016-01-01', end: ''}
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

            this.update(this.props.stock, this.state)
        },

        componentWillUpdate: function (nextProps, nextState) {
            this.update(nextProps.stock, nextState)
        },

        update: function(stock, state) {
            var name = this.props.stock.name;
            $.get('/stock_betas/' + this.props.market + '/' + stock.symbol, state, function (data) {
                if (data.length == 0) {
                    alert('No data ;_;')
                } else {
                    lineChart(data.dates, data.betas)
                }
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
            return <div id="left-nav">
                <div id='stock-search'>
                    <input type="text"
                           className={"form-control"}
                           placeholder={"Search " + MARKETS[this.props.market] + "..."}
                           onKeyUp={this.props.handleKeyUp} />
                </div>
                <div id='stock-list'>
                    {this.props.stocks.map(function (s) {
                        var klass = s.symbol == that.props.selected.symbol ? 'selected' : '';
                        return <a key={s.symbol}
                                  href={'#' + s.symbol}
                                  onClick={that.props.handleClick.bind(that, s)}
                                  className={klass}>
                            {s.name} ({s.symbol})
                        </a>
                    })}
                </div>
            </div>
        }
    });

    function lineChart(dates, betas) {
        if (betas.length == 0) {
            $('#chart').empty().text('No data...')
            return
        }

        var chart = c3.generate({
            bindto: '#chart',
            data: {
                x: 'x',
                columns: [dates, betas]
            },
            axis: {
                x: {
                    type: 'timeseries',
                    tick: {
                        format: '%Y-%m-%d',
                        count: 10,
                    }
                },
                y: {
                    tick: {
                        format: d3.format('.2f'),
                    }
                }
            },
            grid: {
                y: {
                    show: true
                }
            },
            point: {
                show: false
            }
        });
    }

    ReactDOM.render(<App />, document.getElementsByTagName('body')[0]);
})()


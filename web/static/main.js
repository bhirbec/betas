(function () {

    var timeout;

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
        componentDidMount: function() {
            this.update('')
        },

        componentWillReceiveProps: function (nextProps) {
            this.update(this.state.q, nextProps.market)
        },

        update: function (q, market) {
            var market =  market || this.props.market;

            var that = this;
            $.get('/stock_list/' + market, {q: q}, function (stocks) {
                that.setState({stocks: stocks, selected: 0, q: q});
            });
        },

        handleSearch: function(e) {
            clearTimeout(timeout);

            var q = e.target.value;
            if (q == this.state.q) {
                return
            }

            var that = this;
            timeout = setTimeout(function() {that.update(q)}, 200);
        },

        handleArrow: function (e) {
            var $n = $(ReactDOM.findDOMNode(this));
            var $selected = $n.find('#stock-list a.selected');
            var i = -1;

            if (e.keyCode == 40) {
                i = ($selected.next().attr('href') || '').substring(1);
            } else if (e.keyCode == 38) {
                i = ($selected.prev().attr('href') || '').substring(1);
            } else {
                return
            }

            if (i != '') {
                this.setState({selected: i});
            }
        },

        handleClick: function(i, e) {
            this.setState({selected: i})
            e.preventDefault();
        },

        render: function() {
            if ($.isEmptyObject(this.state)) {
                return null
            } else {
                return <div id={"content"}>
                    <div className={"scroll"}>
                        <StockList stocks={this.state.stocks}
                                   market={this.props.market}
                                   selected={this.state.selected}
                                   handleClick={this.handleClick}
                                   handleSearch={this.handleSearch}
                                   handleArrow={this.handleArrow} />
                        <Report stock={this.state.stocks[this.state.selected] || {}} market={this.props.market} />
                    </div>
                </div>
            }
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
                           onChange={this.props.handleSearch}
                           onKeyUp={this.props.handleArrow} />
                </div>
                <div id='stock-list'>
                    {this.props.stocks.map(function (s, i) {
                        var klass = i == that.props.selected ? 'selected' : '';
                        return <a key={s.symbol}
                                  href={'#' + i}
                                  onClick={that.props.handleClick.bind(that, i)}
                                  className={klass}>
                            {s.name} ({s.symbol})
                        </a>
                    })}
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
                lineChart(data.dates, data.betas)
            });
        },

        render: function () {
            if ($.isEmptyObject(this.props.stock)) {
                return <div id="result">
                    <h2>No Match...</h2>
                </div>
            } else {
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


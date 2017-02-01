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

        shouldComponentUpdate: function (nextProps, nextState) {
            return this.props.stock.symbol != nextProps.stock.symbol
        },

        render: function () {
            if ($.isEmptyObject(this.props.stock)) {
                return <div id="result">
                    <h2>No Match...</h2>
                </div>
            } else {
                return <div id="result">
                    <h2>{this.props.stock.name} ({this.props.stock.symbol})</h2>
                    <TimeSerieChart url={ '/stock_betas/' + this.props.market + '/' + this.props.stock.symbol } />
                </div>
            }
        }
    });

    var TimeSerieChart = React.createClass({

        getData: function(url, state) {
            $.get(url, state, function (data) {
                lineChart(data.dates, data.betas)
            });
        },

        render: function () {
            return <div>
                <DateWidget url={this.props.url } getData={ this.getData } />
                <div id="chart"></div>
            </div>
        }
    })

    var DateWidget = React.createClass({
        getInitialState: function () {
            return {
                start: formatDate(this.dateDelta('1y')),
                end: formatDate(new Date()),
            }
        },

        componentWillReceiveProps: function (newProps) {
            // reset to initial state when we change stock
            this.update(newProps.url, this.state)
        },

        componentDidMount: function () {
            var that = this;
            var $n = $(ReactDOM.findDOMNode(this));

            $n.find('.datepicker').datepicker({
                format: "yyyy-mm-dd",
                autoclose: true
            })

            $n.find('#start-date').on('changeDate', function (e) {
                that.state['start'] = e.target.value
                that.update(that.props.url, that.state)
            });

            $n.find('#end-date').on('changeDate', function (e) {
                that.state['end'] = e.target.value
                that.update(that.props.url, that.state)
            });

            this.update(this.props.url, this.state)
        },

        update: function(url, state) {
            this.setState(state);
            this.props.getData(url, state)
        },

        dateDelta: function (q) {
            var handlers = {
                '1w': 7,
                '1m': 31,
                '1y': 365,
                '2y': 2*365,
                '5y': 5*365,
            };

            var now = new Date();
            now.setDate(now.getDate() - handlers[q]);
            return now
        },

        handleChange: function (key, e) {
            var state = {}
            state[key] = e.target.value
            this.update(this.props.url, state)
            e.preventDefault();
        },

        handleClick: function (q, e) {
            if (q != 'range') {
                var start = this.dateDelta(q)
                var end = new Date()

                var state = {
                    start: formatDate(start),
                    end: formatDate(end),
                    value: q,
                }

                var $n = $(ReactDOM.findDOMNode(this));
                $n.find('#start-date').datepicker("setDate", start);
                $n.find('#end-date').datepicker("setDate", end);

                this.update(this.props.url, state)
            } else {
                this.setState({value: 'range'})
            }

            e.preventDefault()
        },

        render: function() {
            var value = this.state.value || '1y'

            return <div id="dates-form">
                <div style={ {display: value == 'range' ? 'block' : 'none'} }>
                    <label>From: </label>
                    <input key={'start'}
                           id="start-date"
                           type="text"
                           className={"datepicker"}
                           value={this.state.start}
                           onChange={this.handleChange.bind(this, 'start')} />
                    <label>To: </label>
                    <input key={'end'}
                           id="end-date"
                           type="text"
                           className={"datepicker"}
                           value={this.state.end}
                           onChange={this.handleChange.bind(this, 'end')} />
                    <a href="#" onClick={this.handleClick.bind(this, '1y')}>back</a>
                </div>
                <div style={ {display: value == 'range' ? 'none' : 'block'} }>
                    <a href="#" onClick={this.handleClick.bind(this, 'range')} className={selected(value, 'range')}>Date range</a>
                    <a href="#" onClick={this.handleClick.bind(this, '5y')} className={selected(value, '5y')}>5 Y</a>
                    <a href="#" onClick={this.handleClick.bind(this, '2y')} className={selected(value, '2y')}>2 Y</a>
                    <a href="#" onClick={this.handleClick.bind(this, '1y')} className={selected(value, '1y')}>1 Y</a>
                    <a href="#" onClick={this.handleClick.bind(this, '1m')} className={selected(value, '1m')}>1 M</a>
                    <a href="#" onClick={this.handleClick.bind(this, '1w')} className={selected(value, '1w')}>1 W</a>
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

    function formatDate(date) {
        var day = date.getDate();
        var month = date.getMonth();
        var year = date.getFullYear();
        return year + '-' + leftPad(month+1, '00') + '-' + leftPad(day, '00');
    }

    function leftPad(str, pad) {
        str += '';
        return pad.substring(0, pad.length - str.length) + str
    }

    function selected(value, selected, attr) {
        return value == selected ? (attr || 'selected') : ''
    }

    ReactDOM.render(<App />, document.getElementsByTagName('body')[0]);
})()


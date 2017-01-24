(function () {
    function loadApp() {
        $.get('/stock_list', function (stocks) {
            ReactDOM.render(<App stocks={stocks} selected={stocks[0]} />, document.getElementById('app'));
        });
    }

    var App = React.createClass({
        componentDidMount: function () {
            var that = this
            var n = ReactDOM.findDOMNode(this);

            $(n).find('.datepicker').datepicker({
                format: "yyyy-mm-dd",
                autoclose: true,
            }).on('changeDate', function (e) {
                renderChart(that.props.selected)
            })
        },

        render: function() {
            return <div>
                <div id="left-nav">
                    <StockList stocks={this.props.stocks} selected={this.props.selected} />
                </div>
                <div id="result">
                    <label>From: </label>
                    <input id="start-date" type="text" className={"datepicker"} />
                    <label>To: </label>
                    <input id="end-date" type="text" className={"datepicker"} />
                    <div id="chart"></div>
                </div>
            </div>
        }
    });

    var StockList = React.createClass({
        getInitialState: function () {
            return {selected: this.props.selected}
        },

        componentDidMount: function () {
            renderChart(this.state.selected);
        },

        handleClick: function(stock, e) {
            renderChart(stock)
            this.setState({selected: stock})
            e.preventDefault();
        },

        render: function() {
            var that = this;
            return <div id='stock-list'>
                <StockSearch />
                <div>
                    {this.props.stocks.map(function (s) {
                        var klass = s.symbol == that.state.selected.symbol ? 'selected' : '';
                        return <a key={s.symbol} href="#{s.symbol}" onClick={that.handleClick.bind(that, s)} className={klass}>
                            {s.name} ({s.symbol})
                        </a>
                    })}
                </div>
            </div>
        }
    });

    var StockSearch = React.createClass({
        render: function() {
            return <div id='stock-search'>
                <input type="text" />
            </div>
        }
    });

    function renderChart(stock) {
        var params = {
            start: $('#start-date').val(),
            end: $('#end-date').val(),
        }

        $.get('/stock_betas/' + stock.symbol, params, function (data) {
            drawGoogleChart(stock, data);
        });
    }

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


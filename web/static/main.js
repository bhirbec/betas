(function () {
    function loadApp() {
        $.get('/stock_list', function (stocks) {
            ReactDOM.render(<App stocks={stocks} />, document.getElementById('app'));
        });
    }

    var App = React.createClass({
        render: function() {
            return <div>
                <div id="left-nav">
                    <StockList stocks={this.props.stocks} />
                </div>
                <div id="result">
                    <div id="chart"></div>
                </div>
            </div>
        }
    });

    var StockList = React.createClass({
        render: function() {
            var that = this;

            return <div id='stock-list'>
                <StockSearch />
                {this.props.stocks.map(function (s) {
                    return <div key={s.symbol}><StockLink symbol={s.symbol} name={s.name} /></div>
                })}
            </div>
        }
    });

    var StockLink = React.createClass({

        handleClick: function(e) {
            var stock = this.props
            $.get('/stock_betas/' + stock.symbol, function (data) {
                drawGoogleChart(stock, data);
            });

            e.preventDefault();
        },

        render: function() {
            return <a href="#{this.props.symbol}" onClick={this.handleClick}>{this.props.name}</a>
        }
    });

    var StockSearch = React.createClass({
        render: function() {
            return <div id='stock-search'>
                <input type="text" />
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


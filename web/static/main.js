(function () {
    function search() {
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
                    <canvas id="chart"></canvas>
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
            $.get('/stock_betas/' + this.props.symbol, function (data) {
                $('#result').empty();
                $('#result').append('<canvas id="chart"></canvas>');
                createChart(data.dates, data.betas);
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

    function createChart(labels, serie) {
        var data = {
            // labels: ["January", "February", "March", "April", "May", "June", "July"],
            labels: labels,
            datasets: [
                {
                    label: "My First dataset",
                    fill: false,
                    lineTension: 0.1,
                    backgroundColor: "rgba(75,192,192,0.4)",
                    borderColor: "rgba(75,192,192,1)",
                    borderCapStyle: 'butt',
                    borderDash: [],
                    borderDashOffset: 0.0,
                    borderJoinStyle: 'miter',
                    pointBorderColor: "rgba(75,192,192,1)",
                    pointBackgroundColor: "#fff",
                    pointBorderWidth: 1,
                    pointHoverRadius: 5,
                    pointHoverBackgroundColor: "rgba(75,192,192,1)",
                    pointHoverBorderColor: "rgba(220,220,220,1)",
                    pointHoverBorderWidth: 2,
                    pointRadius: 1,
                    pointHitRadius: 10,
                    data: serie,
                    spanGaps: false,
                }
            ]
        };

        var ctx = document.getElementById('chart');

        var myLineChart = new Chart(ctx, {
            type: 'line',
            data: data,
            options: {}
        });

    }

    search();
})()


(function () {
    function search() {
        $.get('/stock_list', function (stocks) {
            ReactDOM.render(<App stocks={stocks} />, document.getElementById('app'));
        });
    }

    var App = React.createClass({
        render: function() {
            return <div>
                <StockList stocks={this.props.stocks} />
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
            $.get('/stock_betas/' + this.props.symbol, function (betas) {
                console.log(betas)
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

    search();
})()


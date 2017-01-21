import datetime
import etl


def test_download_quote():
    start = datetime.date(day=01, month=01, year=2017)
    end = datetime.date(day=10, month=01, year=2017)
    result = [r[0] for r in etl.download_quote('AAPL', start, end)]
    expected = ['2017-01-10', '2017-01-09', '2017-01-06', '2017-01-05', '2017-01-04', '2017-01-03']
    assert result == expected

def test_download_delta():
    start = datetime.date(day=01, month=01, year=2017)
    end = datetime.date(day=6, month=01, year=2017)
    result = [r[0] for r in etl.download_quote('^IXIC', start, end)]

    start = datetime.date(day=7, month=01, year=2017)
    end = datetime.date(day=10, month=01, year=2017)
    result += [r[0] for r in etl.download_quote('^IXIC', start, end)]

    result = sorted(result, reverse=True)
    expected = ['2017-01-10', '2017-01-09', '2017-01-06', '2017-01-05', '2017-01-04', '2017-01-03']
    assert result == expected

def test_rolling_window():
    stock = [dict(date=i, roi=v) for i, v in enumerate(range(10, 20))]
    bench = [dict(date=i, roi=v) for i, v in enumerate(range(0, 10))]
    result = [(d, list(s), list(b)) for d, s, b in etl.iter_rolling_window(stock, bench, size=3)]

    assert result == [
        (2, [10, 11, 12], [0, 1, 2]),
        (3, [13, 11, 12], [3, 1, 2]),
        (4, [13, 14, 12], [3, 4, 2]),
        (5, [13, 14, 15], [3, 4, 5]),
        (6, [16, 14, 15], [6, 4, 5]),
        (7, [16, 17, 15], [6, 7, 5]),
        (8, [16, 17, 18], [6, 7, 8]),
        (9, [19, 17, 18], [9, 7, 8])
    ]

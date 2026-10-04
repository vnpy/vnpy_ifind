import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest


# iFinDPy 不参与测试。数据服务标记为已初始化，避免 THS_iFinDLogin。
class _THSData:
    def __init__(self, errorcode: int, data: pd.DataFrame) -> None:
        self.errorcode: int = errorcode
        self.data: pd.DataFrame = data


class _Ifind:
    minute: pd.DataFrame = pd.DataFrame()
    daily: pd.DataFrame = pd.DataFrame()
    hq_calls: list[tuple[object, ...]] = []
    hf_calls: list[tuple[object, ...]] = []


def _login(username: str, password: str) -> int:
    raise AssertionError("THS_iFinDLogin")


def _hq(*args: object) -> _THSData:
    _Ifind.hq_calls.append(args)
    return _THSData(0, _Ifind.daily)


def _hf(*args: object) -> _THSData:
    _Ifind.hf_calls.append(args)
    return _THSData(0, _Ifind.minute)


_ifindpy: ModuleType = ModuleType("iFinDPy")
_ifindpy.THS_iFinDLogin = _login  # type: ignore[attr-defined]
_ifindpy.THS_HQ = _hq  # type: ignore[attr-defined]
_ifindpy.THS_HF = _hf  # type: ignore[attr-defined]
_ifindpy.THSData = _THSData  # type: ignore[attr-defined]
sys.modules["iFinDPy"] = _ifindpy

from vnpy_ifind.ifind_datafeed import CHINA_TZ, IfindDatafeed  # noqa: E402


_MINUTE: pd.DataFrame = pd.DataFrame([{
    "time": "2024-01-15 10:01",
    "open": 100.5,
    "high": 110.0,
    "low": 90.25,
    "close": 105.0,
    "volume": 12.0,
    "amount": 1300.0,
    "openInterest": 8.0,
}])
_DAILY: pd.DataFrame = pd.DataFrame([{
    "time": "2024-01-15",
    "open": 100.5,
    "high": 110.0,
    "low": 90.25,
    "close": 105.0,
    "volume": 12.0,
    "amount": 1300.0,
    "openInterest": 8.0,
}])
_INDICATORS: str = "open;high;low;close;volume;amount;openInterest"


def _request(symbol: str, exchange: Exchange, interval: Interval) -> HistoryRequest:
    return HistoryRequest(
        symbol=symbol,
        exchange=exchange,
        start=datetime(2024, 1, 15, 9, 0),
        end=datetime(2024, 1, 15, 15, 0),
        interval=interval,
    )


def _feed() -> IfindDatafeed:
    _Ifind.hq_calls.clear()
    _Ifind.hf_calls.clear()
    feed: IfindDatafeed = IfindDatafeed()
    feed.username = "offline-user"
    feed.password = "offline-pass"
    feed.inited = True
    return feed


def _assert_bar(bar: BarData, symbol: str, exchange: Exchange, interval: Interval, bar_dt: datetime) -> None:
    assert bar.symbol == symbol
    assert bar.exchange == exchange
    assert bar.interval == interval
    assert bar.datetime == bar_dt
    assert bar.open_price == 100.5
    assert bar.high_price == 110.0
    assert bar.low_price == 90.25
    assert bar.close_price == 105.0
    assert bar.volume == 12.0


@pytest.mark.parametrize(
    ("symbol", "exchange", "ifind_symbol", "forward_adjust"),
    [
        ("rb2410", Exchange.SHFE, "RB2410.SHF", False),
        ("IF2410", Exchange.CFFEX, "IF2410.CFE", False),
        ("TA501", Exchange.CZCE, "TA501.CZC", False),
        ("i2501", Exchange.DCE, "I2501.DCE", False),
        ("sc2501", Exchange.INE, "SC2501.SHF", False),
        ("600000", Exchange.SSE, "600000.SH", True),
        ("000001", Exchange.SZSE, "000001.SZ", True),
    ],
)
def test_minute_exchange_and_bar(
    symbol: str,
    exchange: Exchange,
    ifind_symbol: str,
    forward_adjust: bool,
) -> None:
    assert Path(sys.modules["vnpy_ifind.ifind_datafeed"].__file__ or "").resolve().is_relative_to(
        Path(__file__).resolve().parents[1]
    )
    _Ifind.minute = _MINUTE
    feed: IfindDatafeed = _feed()
    logs: list[str] = []
    bars: list[BarData] = feed.query_bar_history(
        _request(symbol, exchange, Interval.MINUTE),
        output=logs.append,
    )

    params: str = "Fill:Original"
    if forward_adjust:
        params += ",CPS:2"
    params += ",Interval:1"

    assert logs == []
    assert _Ifind.hq_calls == []
    assert _Ifind.hf_calls == [(
        ifind_symbol,
        _INDICATORS,
        params,
        "2024-01-15 09:00:00",
        "2024-01-15 15:00:00",
    )]
    assert len(bars) == 1
    _assert_bar(
        bars[0],
        symbol,
        exchange,
        Interval.MINUTE,
        datetime(2024, 1, 15, 10, 0, tzinfo=CHINA_TZ),
    )


def test_daily_stock_bar() -> None:
    _Ifind.daily = _DAILY
    feed: IfindDatafeed = _feed()
    logs: list[str] = []
    bars: list[BarData] = feed.query_bar_history(
        _request("600000", Exchange.SSE, Interval.DAILY),
        output=logs.append,
    )

    assert logs == []
    assert _Ifind.hf_calls == []
    assert _Ifind.hq_calls == [(
        "600000.SH",
        _INDICATORS,
        "Fill:Original,CPS:2",
        "2024-01-15 09:00:00",
        "2024-01-15 15:00:00",
    )]
    assert len(bars) == 1
    _assert_bar(
        bars[0],
        "600000",
        Exchange.SSE,
        Interval.DAILY,
        datetime(2024, 1, 15, tzinfo=CHINA_TZ),
    )

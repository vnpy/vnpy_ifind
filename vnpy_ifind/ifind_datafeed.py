from datetime import timedelta
from typing import List, Optional
from pytz import timezone

from pandas import DataFrame
from iFinDPy import (
    THS_iFinDLogin,
    THS_HQ,
    THS_HF,
    THS_SS
)

from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.utility import extract_vt_symbol
from vnpy.trader.datafeed import BaseDatafeed


CHINA_TZ = timezone("Asia/Shanghai")

EXCHANGE_MAP = {
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ",
    Exchange.CFFEX: "CFE",
    Exchange.SHFE: "SHF",
    Exchange.CZCE: "CZC",
    Exchange.DCE: "DCE",
}

INTERVAL_MAP = {
    Interval.MINUTE: "1",
    Interval.HOUR: "60"
}

SHIFT_MAP = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
}


class IfindDatafeed(BaseDatafeed):
    """同花顺iFinD数据服务接口"""

    def __init__(self) -> None:
        """"""
        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]

        self.inited = False

    def init(self) -> bool:
        """初始化"""
        if self.inited:
            return True

        r = THS_iFinDLogin(self.username, self.password)
        if r:
            return False
        return True

    def query_bar_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """查询K线数据"""
        if not w.isconnected():
            self.init()

        ifind_exchange = EXCHANGE_MAP[req.exchange]
        ifind_symbol = f"{req.symbol}.{ifind_exchange}"

        indicators = "open;high;low;close;volume"

        if req.interval == Interval.DAILY:
            error, df = THS_HQ(
                Codes=ifind_symbol,
                Indicators=indicators,
                startdate=req.start.strftime("%Y-%m-%d %H:%M:%S"),
                enddate=req.end.strftime("%Y-%m-%d %H:%M:%S"),
            )
        else:
            error, df = THS_HF(
                Codes=ifind_symbol,
                Indicators=indicators,
                startdate=req.start.strftime("%Y-%m-%d %H:%M:%S"),
                enddate=req.end.strftime("%Y-%m-%d %H:%M:%S"),
            )

        if error:
            return []

        bars: List[BarData] = []
        for tp in df.itertuples():
            bar = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=tp.Index,
                open_price=tp.open,
                high_price=tp.high,
                low_price=tp.low,
                close_price=tp.close,
                # volume=tp.volume,
                # turnover=tp.turnover,
                # open_interest=tp.oi,
                gateway_name="IFIND"
            )
            bars.append(bar)

        return bars

    def query_tick_history(self, req: HistoryRequest) -> Optional[List[TickData]]:
        """查询Tick数据"""
        if not w.isconnected():
            self.init()

        ifind_exchange = EXCHANGE_MAP[req.exchange]
        ifind_symbol = f"{req.symbol}.{ifind_exchange}"

        indicators = (
            "open;high;low;last;volume;turnover;oi;"
            "bid1;bid2;bid3;bid4;bid5;"
            "ask1;ask2;ask3;ask4;ask5;"
        )

        df: DataFrame = THS_SS(
            codes=ifind_symbol,
            Indicators=indicators,
            startdate=req.start.strftime("%Y-%m-%d %H:%M:%S"),
            enddate=req.end.strftime("%Y-%m-%d %H:%M:%S"),
        )

        ticks: List[TickData] = []
        for tp in df.itertuples():
            tick = TickData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=tp.Index,
                open_price=tp.open,
                high_price=tp.high,
                low_price=tp.low,
                last_price=tp.last,
                volume=tp.volume,
                turnover=tp.turnover,
                open_interest=tp.oi,
                bid_price_1=tp.bid1,
                bid_price_2=tp.bid2,
                bid_price_3=tp.bid3,
                bid_price_4=tp.bid4,
                bid_price_5=tp.bid5,
                ask_price_1=tp.ask1,
                ask_price_2=tp.ask2,
                ask_price_3=tp.ask3,
                ask_price_4=tp.ask4,
                ask_price_5=tp.ask5,
                gateway_name="IFIND"
            )
            ticks.append(tick)

        return ticks

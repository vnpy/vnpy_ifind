from datetime import timedelta, datetime
from typing import List, Optional
from pytz import timezone

from iFinDPy import (
    THS_iFinDLogin,
    THS_HQ,
    THS_HF,
    
)

from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest
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

        code: int = THS_iFinDLogin(self.username, self.password)
        if code:
            return False
        return True

    def query_bar_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """查询K线数据"""
        if not self.inited:
            self.init()

        ifind_exchange = EXCHANGE_MAP[req.exchange]
        ifind_symbol = f"{req.symbol}.{ifind_exchange}"

        shift = SHIFT_MAP.get(req.interval, None)
        indicators = "open;high;low;close;volume;amount;openInterest"

        if req.interval == Interval.DAILY:
            params = "Fill:Original"
            result = THS_HQ(
                ifind_symbol,
                indicators,
                params,
                req.start.strftime("%Y-%m-%d %H:%M:%S"),
                req.end.strftime("%Y-%m-%d %H:%M:%S"),
            )
        else:
            ifind_interval = INTERVAL_MAP[req.interval]
            params = f"Fill:Original,Interval:{ifind_interval}"

            result = THS_HF(
                ifind_symbol,
                indicators,
                params,
                req.start.strftime("%Y-%m-%d %H:%M:%S"),
                req.end.strftime("%Y-%m-%d %H:%M:%S"),
            )

        if result.errorcode:
            return []

        bars: List[BarData] = []
        for tp in result.data.itertuples():
            if ":" in tp.time:
                dt = datetime.strptime(tp.time, "%Y-%m-%d %H:%M")
            else:
                dt = datetime.strptime(tp.time, "%Y-%m-%d")

            if shift:
                dt -= shift

            if tp.openInterest:
                open_interest = tp.openInterest
            else:
                open_interest = 0

            bar = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=CHINA_TZ.localize(dt),
                interval=req.interval,
                open_price=tp.open,
                high_price=tp.high,
                low_price=tp.low,
                close_price=tp.close,
                volume=tp.volume,
                turnover=tp.amount,
                open_interest=open_interest,
                gateway_name="IFIND"
            )
            bars.append(bar)

        return bars

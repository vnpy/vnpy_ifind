from datetime import timedelta, datetime
from typing import Dict, List, Optional, Callable

from iFinDPy import (
    THS_iFinDLogin,
    THS_HQ,
    THS_HF,
    THSData
)

from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.utility import ZoneInfo


CHINA_TZ = ZoneInfo("Asia/Shanghai")

EXCHANGE_MAP: Dict[Exchange, str] = {
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ",
    Exchange.CFFEX: "CFE",
    Exchange.SHFE: "SHF",
    Exchange.CZCE: "CZC",
    Exchange.DCE: "DCE",
    Exchange.INE: "SHF",
}

INTERVAL_MAP: Dict[Interval, str] = {
    Interval.MINUTE: "1",
    Interval.HOUR: "60"
}

SHIFT_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
}


class IfindDatafeed(BaseDatafeed):
    """同花顺iFinD数据服务接口"""

    def __init__(self) -> None:
        """"""
        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]

        self.inited: bool = False

    def init(self, output: Callable = print) -> bool:
        """初始化"""
        if self.inited:
            return True
        
        if not self.username:
            output("iFinD数据服务初始化失败，用户名为空！")
            return False

        if not self.password:
            output("iFinD数据服务初始化失败，密码为空！")
            return False

        code: int = THS_iFinDLogin(self.username, self.password)
        if code:
            output(f"iFinD数据服务初始化失败，错误码：{code}")
            return False
        return True

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> Optional[List[BarData]]:
        """查询K线数据"""
        # 检查是否登录
        if not self.inited:
            self.init(output)

        # 生成iFinD合约代码
        ifind_exchange: str = EXCHANGE_MAP[req.exchange]
        ifind_symbol: str = f"{req.symbol.upper()}.{ifind_exchange}"

        # 计算时间戳平移值
        shift: timedelta = SHIFT_MAP.get(req.interval, None)

        # 查询数据内容
        indicators: str = "open;high;low;close;volume;amount;openInterest"

        # 日线数据
        if req.interval == Interval.DAILY:
            params: str = "Fill:Original"
            result: THSData = THS_HQ(
                ifind_symbol,
                indicators,
                params,
                req.start.strftime("%Y-%m-%d %H:%M:%S"),
                req.end.strftime("%Y-%m-%d %H:%M:%S"),
            )
        # 日内数据
        elif req.interval in INTERVAL_MAP:
            # 生成iFinD数据周期
            ifind_interval: str = INTERVAL_MAP[req.interval]
            params: str = f"Fill:Original,Interval:{ifind_interval}"

            result: THSData = THS_HF(
                ifind_symbol,
                indicators,
                params,
                req.start.strftime("%Y-%m-%d %H:%M:%S"),
                req.end.strftime("%Y-%m-%d %H:%M:%S"),
            )
        # 其他周期数据
        else:
            return []

        # 如果报错则直接返回空值
        if result.errorcode:
            output(f"历史数据查询失败，错误码：{result.errorcode}")
            return []

        # 解析成K线数据
        bars: List[BarData] = []

        for tp in result.data.itertuples():
            # 生成时间戳
            if ":" in tp.time:
                dt: datetime = datetime.strptime(tp.time, "%Y-%m-%d %H:%M")
            else:
                dt: datetime = datetime.strptime(tp.time, "%Y-%m-%d")

            # 检查时间戳平移
            if shift:
                dt -= shift

            # 获取持仓量
            if tp.openInterest:
                open_interest = tp.openInterest
            else:
                open_interest = 0

            # 生成K线对象
            bar: BarData = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=dt.replace(tzinfo=CHINA_TZ),
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

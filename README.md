# VeighNa框架的同花顺iFinD数据服务接口

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.0-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7-blue.svg" />
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

## 说明

基于同花顺iFinDPy接口的1.10.21.4版本开发，支持以下中国金融市场的K线数据（盘后更新）：

* 期货和期货期权：
  * CFFEX：中国金融期货交易所
  * SHFE：上海期货交易所
  * DCE：大连商品交易所
  * CZCE：郑州商品交易所
* 股票和ETF期权：
  * SSE：上海证券交易所
  * SZSE：深圳证券交易所

注意：需要购买相应的数据服务权限才能获得iFinDPy包，可以通过[该页面](http://ft.10jqka.com.cn/)购买。


## 安装

安装环境推荐基于3.0.0版本以上的【[**VeighNa Studio**](https://www.vnpy.com)】。

直接使用pip命令：

```
pip install vnpy_ifind
```


或者下载源代码后，解压后在cmd中运行：

```
pip install .
```

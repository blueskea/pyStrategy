# encoding: UTF-8

from ctaTemplate import *

exch_symbols = {
    # 上海期货交易所 - 20
    "SHFE": [
        "ag",  # 白银
        "al",  # 铝
        "au",  # 黄金
        "bc",  # 铜(BC)
        "bu",  # 石油沥青
        "cu",  # 铜
        "fu",  # 燃料油
        "hc",  # 热轧卷板
        "lu",  # 低硫燃料油
        "ni",  # 镍
        "nr",  # 20号胶
        "pb",  # 铅
        "rb",  # 螺纹钢
        "ru",  # 天然橡胶
        "sc",  # 原油
        "sn",  # 锡
        "sp",  # 纸浆
        "ss",  # 不锈钢
        "wr",  # 线材
        "zn",  # 锌
    ],
    # 大连商品交易所 - 21
    "DCE": [
        "a",  # 豆一
        "b",  # 豆二
        "bb",  # 胶合板
        "c",  # 玉米
        "cs",  # 玉米淀粉
        "eb",  # 苯乙烯
        "eg",  # 乙二醇
        "fb",  # 纤维板
        "i",  # 铁矿石
        "j",  # 焦炭
        "jd",  # 鸡蛋
        "jm",  # 焦煤
        "l",  # 聚乙烯(塑料)
        "lh",  # 生猪
        "m",  # 豆粕
        "p",  # 棕榈油
        "pg",  # 液化石油气(LPG)
        "pp",  # 聚丙烯苯(PP)
        "rr",  # 粳米
        "v",  # 聚氯乙烯(PVE)
        "y",  # 豆油
    ],
    # 郑州商品交易所 - 23
    "CZCE": [
        "AP",  # 苹果
        "CF",  # 棉花
        "CJ",  # 红枣
        "CY",  # 棉纱
        "FG",  # 玻璃
        "JR",  # 粳稻
        "LR",  # 晚籼稻
        "MA",  # 甲醇
        "OI",  # 菜籽油
        "PF",  # 短纤
        "PK",  # 花生
        "PM",  # 普麦
        "RI",  # 早籼稻
        "RM",  # 菜籽粕
        "RS",  # 油菜籽
        "SA",  # 纯碱
        "SF",  # 硅铁
        "SM",  # 锰硅
        "SR",  # 白糖
        "TA",  # PTA
        "UR",  # 尿素
        "WH",  # 强麦
        "ZC",  # 动力煤
    ]
}


class A_Tick_Crawler(CtaTemplate):
    """Tick Crawler"""
    className = 'A_Tick_Crawler'
    author = 'Jonas Dong'

    def __init__(self) -> None:
        super().__init__()
        
        self.__init_storage()
        self.__init_paramVar()

        self.vtSymbol, self.exchange = self.__query_all_symbols()  # set default parameters
        self.fresh_count = 500
        self.start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        self.count = 0

    def __init_paramVar(self):
        # 参数映射表: 动态设置参数用
        self.paramMap = {
            "vtSymbol": "合约列表",
            "exchange": "交易所",
            "save_path": "数据保存路径",
            "fresh_count": "刷新频率"
        }

        # 变量映射表: 显示在UI上用
        self.varMap = {
            "start_time": "StartTime",
            "count": "Count"
        }


    def __init_storage(self):
        # create data directory
        home_path = os.path.expanduser("~").replace("\\", "/")  # C:/Users/jonas
        self.save_path = home_path + "/Desktop/data"
        os.makedirs(self.save_path, exist_ok=True)

        # delete last config file
        config_path = home_path + "/AppData/Roaming/InfiniTrader_QhFangzhengzhongqi/pyStrategy/json/crawler.json"
        if os.path.exists(config_path):
            os.remove(config_path)

    # all contracts of 3 main exchange
    def __query_all_symbols(self):
        contract_list = []
        exchange_list = []
        for exch, codes in exch_symbols.items():
            for code in codes:
                contracts = self.get_InstListByExchAndProduct(exch, code)  # {'1': ['cu2210', 'cu2207', 'cuMain']}
                if not contracts:
                    self.output(f"exch={exch}, code={code}: no contracts")
                    continue
                contracts = list(contracts.values())[0]  # ['cu2210', 'cu2207', 'cuMain']
                contracts = list(filter(lambda name: "Main" not in name, contracts))  # ['cu2210', 'cu2207']
                for contract in contracts:
                    contract_list.append(contract)
                    exchange_list.append(exch)
        # self.output(f"contracts are: {','.join(contract_list)}")
        self.output(f"contracts count: {len(contract_list)}")
        return ";".join(contract_list), ";".join(exchange_list)

    def onTick(self, tick):
        try:
            line = self.build_line(tick)
        except Exception as e:
            return
        # if tick.lastPrice == 0 or tick.askPrice1 == 0 or tick.bidPrice1 == 0:  # 涨跌停和集合竞价
        #    self.output(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: passed tick: {line}")
        self.csv.write(line)
        self.count = self.count + 1
        if self.count % self.fresh_count == 0:
            self.putEvent()

    def keep_float(self, num: float):
        return int(num * 100) / 100

    def build_line(self, tick):
        line = (f"{tick.exchange}.{tick.symbol},"  # 合约代码.交易所代码
                # f"{tick.date} {tick.time},"  # 时间 20220511 11:20:56.5 type=str
                f"{tick.datetime.strftime('%Y-%m-%d %H:%M:%S.%f')},"  # 时间 2022-05-12 21:18:35.540000
                f"{self.keep_float(tick.lastPrice)},"  # 最新成交价
                f"{tick.lastVolume},"  # 最新成交量
                f"{tick.volume},"  # 今天总成交量
                f"{tick.openInterest},"  # 持仓量
                f"{self.keep_float(tick.openPrice)},"  # 今日开盘价
                f"{self.keep_float(tick.highPrice)},"  # 今日最高价
                f"{self.keep_float(tick.lowPrice)},"  # 今日最低价
                f"{self.keep_float(tick.preClosePrice)},"  # 昨收盘价
                f"{self.keep_float(tick.PreSettlementPrice)},"  # 昨结算价
                f"{self.keep_float(tick.upperLimit)},"  # 涨停价
                f"{self.keep_float(tick.lowerLimit)},"  # 跌停价
                f"{self.keep_float(tick.turnover)},"  # 成交额
                f"{self.keep_float(tick.bidPrice1)},"
                f"{self.keep_float(tick.bidPrice2)},"
                f"{self.keep_float(tick.bidPrice3)},"
                f"{self.keep_float(tick.bidPrice4)},"
                f"{self.keep_float(tick.bidPrice5)},"
                f"{self.keep_float(tick.askPrice1)},"
                f"{self.keep_float(tick.askPrice2)},"
                f"{self.keep_float(tick.askPrice3)},"
                f"{self.keep_float(tick.askPrice4)},"
                f"{self.keep_float(tick.askPrice5)},"
                f"{tick.bidVolume1},"
                f"{tick.bidVolume2},"
                f"{tick.bidVolume3},"
                f"{tick.bidVolume4},"
                f"{tick.bidVolume5},"
                f"{tick.askVolume1},"
                f"{tick.askVolume2},"
                f"{tick.askVolume3},"
                f"{tick.askVolume4},"
                f"{tick.askVolume5}"
                "\n")
        return line

    def onBar(self, bar):
        pass

    def onTrade(self, trade, log=True):
        self.output(f"onTrade: {trade}")

    def onOrder(self, order, log=False):
        self.output(f"onOrder: {order}")

    def onStart(self):
        self.csv_path = f"{self.save_path}/{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.csv = open(self.csv_path, "w")
        headers = ("symbol,"
                   "datetime,"
                   "lastPrice,"
                   "lastVolume,"
                   "volume,"
                   "openInterest,"
                   "openPrice,"
                   "highPrice,"
                   "lowPrice,"
                   "preClosePrice,"
                   "PreSettlementPrice,"
                   "upperLimit,"
                   "lowerLimit,"
                   "turnover,"
                   "bidPrice1,"
                   "bidPrice2,"
                   "bidPrice3,"
                   "bidPrice4,"
                   "bidPrice5,"
                   "askPrice1,"
                   "askPrice2,"
                   "askPrice3,"
                   "askPrice4,"
                   "askPrice5,"
                   "bidVolume1,"
                   "bidVolume2,"
                   "bidVolume3,"
                   "bidVolume4,"
                   "bidVolume5,"
                   "askVolume1,"
                   "askVolume2,"
                   "askVolume3,"
                   "askVolume4,"
                   "askVolume5"
                   "\n")
        self.csv.write(headers)
        self.csv.flush()
        self.output(f"save csv to: {self.csv_path}")
        super().onStart()

    def onStop(self):
        if self.csv and not self.csv.closed:
            self.csv.flush()
            self.csv.close()
            self.output(f"finish and close csv in: {self.csv_path}")
        super().onStop()

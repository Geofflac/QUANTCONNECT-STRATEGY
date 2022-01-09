class EtfCurrFuturesMomentum(QCAlgorithm):

    def Initialize(self):
        self.SetStartDate(2019, 10, 1)
        #self.SetEndDate(2019, 10, 1)
        self.SetCash(100000)
        self.curr_data = {}
        self.etf_data = {}
        self.crypto_data = {}
        self.tech_data = {}
        self.energy_data = {}
        self.period = 4 * 5
        self.curr_leverage = 2
        self.curr_count = 1
        self.etf_count = 4
        self.crypto_count = 1
        self.tech_count = 2
        self.energy_count = 1
        self.tp_threshold = 1.20
        self.sl_threshold = 0.9
        self.SetWarmUp(self.period)
        self.etf_symbols = ["SPY", "IVV", "VOO", "QQQ", "IWM", "EWQ", "KGRN", "ECNS", "CHIQ", "INDQ", "EPI", "INDY" ]
        self.crypto_symbols = ["BTCUSD", "ETHUSD", "XPRUSD"]
        self.tech_symbols = ["AAPL", "TSLA", "MSFT", "ORCLA", "DSY.PA"]
        self.energy_symbols = ["XOM", "CVX", "TTE.PA", "ALTVO.PA", "FSLR", "BEP", "VWS.CO", "VERB.VI"]
        self.curr_symbols = [
                        "CME_AD1", # Australian Dollar Futures, Continuous Contract #1
                        "CME_BP1", # British Pound Futures, Continuous Contract #1
                        "CME_CD1", # Canadian Dollar Futures, Continuous Contract #1
                        "CME_EC1", # Euro FX Futures, Continuous Contract #1
                        "CME_JY1", # Japanese Yen Futures, Continuous Contract #1
                        "CME_MP1", # Mexican Peso Futures, Continuous Contract #1
                        "CME_NE1", # New Zealand Dollar Futures, Continuous Contract #1
                        "CME_SF1"  # Swiss Franc Futures, Continuous Contract #1
                        ]
                        
        for symbol in self.curr_symbols:
            data = self.AddData(QuantpediaFutures, symbol, Resolution.Daily)
            #data.SetFeeModel(CustomFeeModel(self))
            data.SetLeverage(self.curr_leverage)
            self.curr_data[symbol] = self.ROC(symbol, self.period, Resolution.Daily)
        
        for symbol in self.crypto_symbols:
            self.AddEquity(symbol, Resolution.Daily)
            self.crypto_data[symbol] = self.ROC(symbol, self.period, Resolution.Daily)

        for symbol in self.etf_symbols:
            self.AddEquity(symbol, Resolution.Daily)
            self.etf_data[symbol] = self.ROC(symbol, self.period, Resolution.Daily)

        for symbol in self.tech_symbols:
            self.AddEquity(symbol, Resolution.Daily)
            self.tech_data[symbol] = self.ROC(symbol, self.period, Resolution.Daily)

        for symbol in self.energy_symbols:
            self.AddEquity(symbol, Resolution.Daily)
            self.energy_data[symbol] = self.ROC(symbol, self.period, Resolution.Daily)

        self.Schedule.On(self.DateRules.WeekStart(self.curr_symbols[0]), self.TimeRules.AfterMarketOpen(self.curr_symbols[0]), self.Rebalance)
    
    def Rebalance(self):
        if self.IsWarmingUp: 
            return
        
        curr_sorted_by_performance = sorted([x for x in self.curr_data.items() if x[1].IsReady], key = lambda x: x[1].Current.Value, reverse = True)
        curr_long = [x[0] for x in curr_sorted_by_performance[:self.curr_count]]
        
        sorted_by_momentum = sorted([x for x in self.etf_data.items() if x[1].IsReady], key = lambda x: x[1].Current.Value, reverse = True)
        etf_long = [x[0] for x in sorted_by_momentum][:self.etf_count]
        
        sorted_by_momentum_crypto = sorted([x for x in self.crypto_data.items() if x[1].IsReady], key = lambda x: x[1].Current.Value, reverse = True)
        crypto_long = [x[0] for x in sorted_by_momentum_crypto][:self.crypto_count]
        
        sorted_by_momentum_tech = sorted([x for x in self.tech_data.items() if x[1].IsReady], key = lambda x: x[1].Current.Value, reverse = True)
        tech_long = [x[0] for x in sorted_by_momentum_tech][:self.tech_count]
        
        sorted_by_momentum_energy = sorted([x for x in self.energy_data.items() if x[1].IsReady], key = lambda x: x[1].Current.Value, reverse = True)
        energy_long = [x[0] for x in sorted_by_momentum_energy][:self.tech_count]
        
        long = curr_long + etf_long + crypto_long + tech_long + energy_long

        # Trade execution.
        invested = [x.Key.Value for x in self.Portfolio if x.Value.Invested]
        for symbol in invested:
            if symbol not in long:
                self.Liquidate(symbol)
                
        for symbol in long:
            self.SetHoldings(symbol, 1 / len(long))
    
    def OnData(self, data):
        invested = [x.Symbol.Value for x in self.Portfolio.Values if x.Invested]
        for symbol in invested:
            if data.ContainsKey(symbol) and data[symbol]:
                if data[symbol].Price < self.sl_threshold*self.Portfolio[symbol].AveragePrice:
                    self.Liquidate(symbol)
                    self.Debug(self.Time)
                    self.Debug(symbol)
                    self.Debug("SL executed---------------")
                elif data[symbol].Price > self.tp_threshold*self.Portfolio[symbol].AveragePrice:
                    self.Liquidate(symbol)
                    self.Debug(self.Time)
                    self.Debug(symbol)
                    self.Debug("TP executed---------------")

# Quantpedia data.
# NOTE: IMPORTANT: Data order must be ascending (datewise)
class QuantpediaFutures(PythonData):
    def GetSource(self, config, date, isLiveMode):
        return SubscriptionDataSource("data.quantpedia.com/backtesting_data/futures/{0}.csv".format(config.Symbol.Value), SubscriptionTransportMedium.RemoteFile, FileFormat.Csv)

    def Reader(self, config, line, date, isLiveMode):
        data = QuantpediaFutures()
        data.Symbol = config.Symbol
        
        if not line[0].isdigit(): return None
        split = line.split(';')
        
        data.Time = datetime.strptime(split[0], "%d.%m.%Y") + timedelta(days=1)
        data['back_adjusted'] = float(split[1])
        data['spliced'] = float(split[2])
        data.Value = float(split[1])

        return data

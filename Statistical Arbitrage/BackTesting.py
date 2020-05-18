#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: chenyuwang
"""
from Database import *
import datetime
import matplotlib.pyplot as plt
import seaborn as sns

class StockGroup:
    def __init__(self, GroupName, Month, Year):
        self.GroupName = GroupName
        self.Month = Month
        self.Year = Year
        self.stock_list = self.getStockList()
        self.volatility = self.getVolatility()
        self.miu = self.getMean()
        self.trading_volume = self.getTradingvolume()
        self.trades = {'Date':[], 'Net_Value':[], 'S_stats':[], 'profit_loss':[], 'Holding':[]}
        self.total_profit_loss = 0.0
    
    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__) + "\n"
    
    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__) + "\n"
      
    def getStockList(self):
        sql_sentence = "SELECT Symbol FROM StockGroups WHERE GroupName='" + self.GroupName + "' AND Year=" + str(self.Year)
        stock_list = list(pd.DataFrame(execute_sql_statement(sql_sentence, engine).fetchall())[0])
        return stock_list
        
    def getTradingvolume(self):
        sql_sentence = "SELECT Symbol, Volume FROM TradingQuantity WHERE GroupName='" + self.GroupName + "' AND Year=" + str(self.Year) + " AND Month=" + str(self.Month)
        volume = pd.DataFrame(execute_sql_statement(sql_sentence, engine).fetchall()).set_index([0])
        trade_volume = {}
        for col in self.stock_list:
            trade_volume[col] = float(volume.loc[col])
        return trade_volume
        
    def getVolatility(self):
        sql_sentence = "SELECT Sigma FROM Portfolio WHERE GroupName='" + self.GroupName + "' AND Year=" + str(self.Year) + " AND Month=" + str(self.Month)
        return float(pd.DataFrame(execute_sql_statement(sql_sentence, engine).fetchall()).iloc[0, 0])*np.sqrt(1/2)
    
    def getMean(self):
        sql_sentence = "SELECT Miu FROM Portfolio WHERE GroupName='" + self.GroupName + "' AND Year=" + str(self.Year) + " AND Month=" + str(self.Month)
        return float(pd.DataFrame(execute_sql_statement(sql_sentence, engine).fetchall()).iloc[0, 0])
    
    def readPricetable(self, Price_Table):
        dic = {}
        for index in Price_Table.index:
            print(index)
            #dic[index[1]] = float(Price_Table[index])
            dic[index] = float(Price_Table[index])
        return dic
    
    
    def createTrade(self, date, Price_Table, profit_loss = 0.0):
        ### Trading table TradingQuantity[['symbol', 'volume']]
        print("Start to read trading table")
        price_info = self.readPricetable(Price_Table)
        Net_Value = 0
        for key in price_info.keys():
            Net_Value += price_info[key] * self.trading_volume[key]
        S_stats = (Net_Value - self.miu)/self.volatility
        self.trades['Date'].append(date)
        self.trades['Net_Value'].append(Net_Value)
        self.trades['S_stats'].append(S_stats)
        self.trades['profit_loss'].append(profit_loss)
        self.trades['Holding'].append(0)
    
    def updateTrades(self):
        trading_matrix = np.array(pd.DataFrame(self.trades))
        position = 0
        for index in range(1, trading_matrix.shape[0]):
            s_ = trading_matrix[index - 1, 2]
            if position == 0:
                if s_ < -1.25:
                    position = 1
                    trading_matrix[index, 3] = trading_matrix[index, 1] - trading_matrix[index - 1, 1]
                    self.total_profit_loss += trading_matrix[index, 3]
                if s_ > 1.25:
                    position = -1
                    trading_matrix[index, 3] = -trading_matrix[index, 1] + trading_matrix[index - 1, 1]
                    self.total_profit_loss += trading_matrix[index, 3]
            elif position == -1:
                if s_ <= 0.9:
                    position = 0
                    trading_matrix[index, 3] = -trading_matrix[index, 1] + trading_matrix[index - 1, 1]
                elif s_ >= 2.5:
                    position = 0
                    trading_matrix[index, 3] = -trading_matrix[index, 1] + trading_matrix[index - 1, 1]
                else:
                    trading_matrix[index, 3] = trading_matrix[index, 1] - trading_matrix[index - 1, 1]
                self.total_profit_loss += trading_matrix[index, 3]
            else:
                if s_>= -0.8:
                    position = 0
                    trading_matrix[index, 3] = trading_matrix[index, 1] - trading_matrix[index - 1, 1]
                elif s_ <= -2.5:
                    position = 0
                    trading_matrix[index, 3] = trading_matrix[index, 1] - trading_matrix[index - 1, 1]
                else:
                    trading_matrix[index, 3] = -trading_matrix[index, 1] + trading_matrix[index - 1, 1]
                self.total_profit_loss += trading_matrix[index, 3]
            trading_matrix[index - 1, 4] = position
        
        return pd.DataFrame(trading_matrix, columns = ['Date', 'Net_Value', 'S_stats', 'profit_loss', 'Holding'])
    
class BackTestingAnalysis:
    def __init__(self, GroupName, result_df=None):
        self.GroupName = GroupName
        self.result_df = result_df
        
    def getCumulativePnL(self, from_database = True):
        if from_database == True:
            select_st = "SELECT Date, Net_Value, S_stats, profit_loss, Holding FROM Trading \
                 WHERE GroupName='" + str(self.GroupName) + "' ORDER BY Date"

            result_set = execute_sql_statement(select_st, engine)
            result_df = pd.DataFrame(result_set.fetchall())
            result_df.columns = result_set.keys()
        else:
            result_df = pd.DataFrame(self.result_df)
        result_df['Cumulative_PnL'] = result_df['profit_loss'].cumsum()
        return result_df
    
    def visualizeSignal(self, from_database = True):
        df = self.getCumulativePnL(from_database)
        if from_database == True:
            df['Date'] = df['Date'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"))
        else:
            df['Date'] = range(df.shape[0])
        fig, ax1 = plt.subplots(figsize=(20, 8))
        ax1.set_ylabel('pnl', color='blue')
        ax1.set_title('BackTesting for ' + self.GroupName, fontsize = 16)
        df.plot('Date', 'Cumulative_PnL', color = 'blue', ax = ax1)
        #df.plot('Date', 'Net_Value', color = 'black', ax = ax1)
        #ax1.plot(df.Cumulative_PnL,'b')
        df['long_position'] = [1 if num == 1 else 0 for num in df.Holding]
        df['short_position'] = [1 if num == -1 else 0 for num in df.Holding]
        ax2 = ax1.twinx() 
        df.plot('Date', 'long_position', color = 'green', alpha = 0.2, ax = ax2)
        #ax2.plot(df.long_position, color = 'green',alpha=0.2)
        ax2.fill_between(df.Date, y1 = [0]*df.shape[0], y2 = df.long_position, color = 'green', alpha = 0.2)
        ax3 = ax1.twinx()
        df.plot('Date', 'short_position', color = 'red', alpha = 0.2, ax = ax3)
        #ax3.plot(df.short_position, color = 'red', alpha = 0.2)
        ax3.fill_between(df.Date, y1 = [0]*df.shape[0], y2 = df.short_position, color = 'red', alpha = 0.2)
        
        ax1.legend(loc='upper center', bbox_to_anchor=(0.75, -0.1), fancybox=True)
        ax2.legend(loc='upper center', bbox_to_anchor=(0.85, -0.1), fancybox=True)
        ax3.legend(loc='upper center', bbox_to_anchor=(0.95, -0.1), fancybox=True)
        plt.show()
        
        
        
        
def bt(metadata, engine, GroupName, Year):
    result = []
    data_matrix = StockPriceQuery(GroupName, Year).set_index(['Date', 'Symbol']).unstack(level = -1)
    total_cost = 0
    for i in range(12):
        stockgroup = StockGroup(GroupName, i + 1, Year)
        ### Get Corresponding Historical Data
        Month = i + 1
        if Month < 10:
            Month_str = str(0) + str(Month)
        else:
            Month_str = str(Month)
        start_date = str(int(Year)) + '-' + Month_str + '-' + '02'
        if Month == 12:
            end_date = str(Year + 1) + '-' + '01' + '-' + '01'
        else:
            Month += 1
            if Month < 10:
                Month_str = str(0) + str(Month)
            else:
                Month_str = str(Month)
            end_date = str(Year) + '-' + Month_str + '-' + '01'
        data_period = data_matrix.loc[start_date : end_date]
        for index, row in data_period.iterrows():
            stockgroup.createTrade(index, row)
        result_df = stockgroup.updateTrades()
        total_cost = stockgroup.total_profit_loss
        result.append(result_df)
    df = pd.concat(result, axis = 0)
    df['GroupName'] = GroupName
    df['Year'] = Year
    df.to_sql('Trading', con=engine, if_exists='append', index=False)

def backtesting(metadata, engine):
    engine.execute('Drop Table if exists Trading;')	
    groups = pd.read_csv(location_of_groups)
    groups = groups.set_index(['GroupName', 'Year']).stack().reset_index()[['GroupName', 'Year', 0]]
    groups.columns = ['GroupName', 'Year', 'Symbol']
    
    GroupName_list = list(groups['GroupName'].unique())
    Year_list = list(groups['Year'].unique())
    for groupname in GroupName_list:
        for year in Year_list:
            bt(metadata, engine, groupname, year)

def visualizeAll(metadata, engine):
    groups = pd.read_csv(location_of_groups)
    groups = groups.set_index(['GroupName', 'Year']).stack().reset_index()[['GroupName', 'Year', 0]]
    groups.columns = ['GroupName', 'Year', 'Symbol']
    
    GroupName_list = list(groups['GroupName'].unique())
    for groupname in GroupName_list:
        BackTestingAnalysis(groupname).visualizeSignal()
    
        
          
"""           
    def updateTrades(self):
        trading_matrix = np.array(pd.DataFrame(self.trades))
        position = 0
        for index in range(1, trading_matrix.shape[0]):
            s_ = trading_matrix[index - 1, 2]
            if position == 0:
                if s_ < -1.25:
                    position = 1
                    trading_matrix[index, 3] = -trading_matrix[index, 1] + trading_matrix[index - 1, 1]
                    self.total_profit_loss += trading_matrix[index, 3]
                if s_ > 1.25:
                    position = -1
                    trading_matrix[index, 3] = trading_matrix[index, 1] - trading_matrix[index - 1, 1]
                    self.total_profit_loss += trading_matrix[index, 3]
            elif position == -1:
                if s_ <= 0.9:
                    position = 0
                else:
                    trading_matrix[index, 3] = trading_matrix[index, 1] - trading_matrix[index - 1, 1]
                self.total_profit_loss += trading_matrix[index, 3]
            else:
                if s_>= -0.8:
                    position = 0
                else:
                    trading_matrix[index, 3] = -trading_matrix[index, 1] + trading_matrix[index - 1, 1]
                self.total_profit_loss += trading_matrix[index, 3]
            trading_matrix[index - 1, 4] = position
        
        return pd.DataFrame(trading_matrix, columns = ['Date', 'Net_Value', 'S_stats', 'profit_loss', 'Holding'])
    """ 

    

    

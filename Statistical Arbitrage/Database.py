#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: chenyuwang
"""
### Import modules
import json
import datetime as dt
import urllib.request
import pandas as pd
import numpy as np

from sqlalchemy import Column, ForeignKey, Integer, Float, String
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import inspect

from sklearn.decomposition import PCA

###  Database Setup
engine = create_engine('sqlite:///StatisticalArbitrage.db')
conn = engine.connect()
conn.execute("PRAGMA foreign_keys = ON")

metadata = MetaData()
metadata.reflect(bind=engine)
print('Database Setup')
### Prepare for downloading data
location_of_groups = 'csv/GroupTrading.csv'

start_date = dt.date(2016, 1, 1)
end_date = dt.datetime.today().strftime('%Y-%m-%d')
    
back_testing_start_date = "2019-12-31"
back_testing_end_date = "2020-01-31"
k = 1
print('Prepare for downloading data')
### Download daily data according to symbol
def get_daily_data(symbol, 
                   start=start_date, 
                   end=end_date, 
                   requestURL='https://eodhistoricaldata.com/api/eod/', 
                   apiKey='5ba84ea974ab42.45160048'):
    symbolURL = str(symbol) + '.US?'
    startURL = 'from=' + str(start)
    endURL = 'to=' + str(end)
    apiKeyURL = 'api_token=' + apiKey
    completeURL = requestURL + symbolURL + startURL + '&' + endURL + '&' + apiKeyURL + '&period=d&fmt=json'
    print(completeURL)
    with urllib.request.urlopen(completeURL) as req:
        data = json.load(req)
        return data

### Create Table    
def create_stockgroup_table(table_name, metadata, engine):
    table = Table(table_name, metadata,
                  Column('GroupName', String(50), primary_key=True, nullable=False),
                  Column('Year', String(4), primary_key=True, nullable=False),
                  Column('Symbol', String(50), primary_key=True, nullable=False),
                  extend_existing=True)
    table.create(engine)

def create_stockprice_table(table_name, metadata, engine):
    tables = metadata.tables.keys()
    if table_name not in tables:
        table = Table(table_name, metadata, 
                      Column('Symbol', String(50), ForeignKey('StockPrice.Symbol'), primary_key=True, nullable=False),
                      Column('Date', String(50), primary_key=True, nullable=False),
                      Column('Open', Float, nullable=False),
                      Column('High', Float, nullable=False),
                      Column('Low', Float, nullable=False),
                      Column('Close', Float, nullable=False),
                      Column('Adjusted_close', Float, nullable=False),
                      Column('Volume', Integer, nullable=False),
                      extend_existing=True)
        table.create(engine)

def create_TradingQuantity_table(table_name, metadata, engine):
    table = Table(table_name, metadata,
                  Column('GroupName', String(50), ForeignKey('TradingQuantity.GroupName'), primary_key=True, nullable=False),
                  Column('Year', String(4), ForeignKey('TradingQuantity.Year'), primary_key=True, nullable=False),
                  Column('Month', String(2), primary_key = True, nullable = False),
                  Column('Symbol', String(50), ForeignKey('TradingQuantity.Symbol'), primary_key=True, nullable=False),
                  Column('Position', Float, nullable = False),
                  Column('Volume', Integer, nullable = False),
                  extend_existing=True)
    table.create(engine)

def create_Portfolio_table(table_name, metadata, engine):
    table = Table(table_name, metadata,
                  Column('GroupName', String(50), ForeignKey('Portfolio.GroupName'), primary_key = True, nullable = False),
                  Column('Year', String(4), ForeignKey('Portfolio.Year'), primary_key = True, nullable = False),
                  Column('Month', String(2), primary_key = True, nullable = False),
                  Column('Miu', Float, nullable = False),
                  Column('Sigma', Float, nullable = False),
                  extend_existing=True)
    table.create(engine)

def create_Trading_table(table_name, metadata, engine):
    table = Table(table_name, metadata,
                  Column('GroupName', String(50), ForeignKey('Trading.GroupName'), primary_key = True, nullable = False),
                  Column('Year', String(4), ForeignKey('Trading.Year'), nullable = False),
                  Column('Date', String(50), primary_key = True, nullable = False),
                  Column('Net_Value', Float, nullable = False),
                  Column('S_stats', Float, nullable = False),
                  Column('profit_loss', Float, nullable = False),
                  Column('Holding', Integer, nullable = False),
                  extend_existing=True)
    table.create(engine)

def create_Result_table(table_name, metadata, engine):
    table = Table(table_name, metadata,
                  Column('GroupName', String(50), ForeignKey('Result.GroupName'), primary_key=True, nullable=False),
                  Column('Year', String(4), ForeignKey('Result.Year'), primary_key = True, nullable = False),
                  Column('PnL', Float, nullable = False),
                  extend_existing=True)
    table.create(engine)
    
### Operations in Database
def clear_a_table(table_name, metadata, engine):
    conn = engine.connect()
    table = metadata.tables[table_name]
    delete_st = table.delete()
    conn.execute(delete_st)

def execute_sql_statement(sql_st, engine):
    result = engine.execute(sql_st)
    return result

def populate_stock_data(tickers, engine, table_name, start_date, end_date):
    column_names = ['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Adjusted_close', 'Volume']
    price_data = []
    for ticker in tickers:
        stock = get_daily_data(ticker, start_date, end_date)
        for stock_data in stock:
            price_data.append([ticker, stock_data['date'], stock_data['open'], stock_data['high'], stock_data['low'], \
                               stock_data['close'], stock_data['adjusted_close'], stock_data['volume']])
    stocks = pd.DataFrame(price_data, columns=column_names)
    stocks.to_sql(table_name, con=engine, if_exists='append', index=False)
 
    
def StockPriceQuery(GroupName, Year):
    select_st = "SELECT Symbol, Date, Close FROM StockPrice \
                 WHERE Symbol in(SELECT Symbol \
                 FROM StockGroups \
                 WHERE GroupName = '" + GroupName + "' AND Year = " + str(Year) + ")"
                
                    
    result_set = execute_sql_statement(select_st, engine)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()
    return result_df

def populate_parameter_data(GroupName, Year):
    data_matrix = StockPriceQuery(GroupName, Year).set_index(['Date', 'Symbol']).unstack(level = -1)
    stock_name = [t[1] for t in data_matrix.columns]
    data_matrix.columns = stock_name
    result = []
    para_list = []
    for i in range(1, 13, 1):
        ### Get Corresponding Historical Data
        Month = i
        if Month < 10:
            Month_str = str(0) + str(Month)
        else:
            Month_str = str(Month)
        start_date = str(int(Year) - 1) + '-' + Month_str + '-' + '01'
        end_date = str(Year) + '-' + Month_str + '-' + '01'
        data_period = data_matrix.loc[start_date : end_date]
        
        return_period = data_period.pct_change().dropna()
        ### Train PCA Model to get Components & Solve Linear System to get Trading Volume
        pca = PCA(n_components = return_period.shape[1])
        pca.fit(np.array(return_period))
        a = pca.components_.T
        a[9] = [1]*10
        b = np.zeros(return_period.shape[1])
        b[9] = 1
        x = np.linalg.solve(a, b)
        
        ### Save Trading Quantity & Volume
        x = pd.DataFrame({'Symbol':stock_name, 'Position':x})
        x['GroupName'] = [GroupName] * x.shape[0]
        x['Year'] = [Year] * x.shape[0]
        x['Month'] = [Month] * x.shape[0]
        x['Volume'] = np.array(x['Position']) / np.array(data_period.iloc[-1, :])
        result.append(x)
        
        ### Compute Historical Portfolio Net Value 
        net_value = np.array(data_period) @ np.array(x['Volume'])
        net_value_df = pd.DataFrame({'GroupName':[GroupName]*data_period.shape[0], 
                                  'Year':[Year]*data_period.shape[0],
                                  'Date':list(data_period.index),
                                  'NetValue':net_value})
        
        ### Compute miu and sigma
        miu = float(net_value_df['NetValue'].mean())
        sigma = float(net_value_df['NetValue'].std())
        para = pd.DataFrame({'GroupName':[GroupName],
                             'Year':[Year],
                             'Month':[Month],
                             'Miu':[miu],
                             'Sigma':[sigma]})
        para_list.append(para)
        
        ### Compute New Portfolio Net Value
        start_date_2 = str(int(Year)) + '-' + Month_str + '-' + '02'
        if Month == 12:
            end_date_2 = str(Year + 1) + '-' + '01' + '-' + '01'
        else:
            Month += 1
            if Month < 10:
                Month_str = str(0) + str(Month)
            else:
                Month_str = str(Month)
            end_date_2 = str(Year) + '-' + Month_str + '-' + '01'
        data_next_period = data_matrix.loc[start_date_2 : end_date_2]
        """
        net_value = np.array(data_next_period) @ np.array(x['Volume'])
        net_value_df = pd.DataFrame({'GroupName':[GroupName]*data_next_period.shape[0], 
                                  'Year':[Year]*data_next_period.shape[0],
                                  'Date':list(data_next_period.index),
                                  'NetValue':net_value})
        
        net_value_df.to_sql('Trading', con = engine, if_exists = 'append', index = False)
        """
        
    result = pd.concat(result, axis = 0)
    result.to_sql('TradingQuantity', con = engine, if_exists = 'append', index = False)
    
    para_df = pd.concat(para_list, axis = 0)
    para_df.to_sql('Portfolio', con = engine, if_exists = 'append', index = False)
    
    print(GroupName + ' ' + str(Year) + ' Parameter Data has been saved in database successfully')

    
    
### Build Database       
def Build_Database():
    print('Start')
    engine.execute('Drop Table if exists StockGroups;')	
    
    create_stockgroup_table('StockGroups', metadata, engine)
    print('Create Group Table Successfully')
    groups = pd.read_csv(location_of_groups)
    groups = groups.set_index(['GroupName', 'Year']).stack().reset_index()[['GroupName', 'Year', 0]]
    groups.columns = ['GroupName', 'Year', 'Symbol']
    groups.to_sql('StockGroups', con=engine, if_exists='append', index=False)
    
    tables = ['StockPrice']
    
    for table in tables:
        create_stockprice_table(table, metadata, engine)
    inspector = inspect(engine)
    
    for table in tables:
        clear_a_table(table, metadata, engine)
    
    populate_stock_data(groups['Symbol'].unique(), engine, 'StockPrice', start_date, end_date)
    print('Downloading Historical Data Successfully')
    
    engine.execute('Drop Table if exists Portfolio;')	
    engine.execute('Drop Table if exists TradingQuantity;')	
    engine.execute('Drop Table if exists Trading;')	
    engine.execute('Drop Table if exists Result;')	
    create_Portfolio_table('Portfolio', metadata, engine)
    create_TradingQuantity_table('TradingQuantity', metadata, engine)
    create_Trading_table('Trading', metadata, engine)
    create_Result_table('Result', metadata, engine)
    
    GroupName_list = list(groups['GroupName'].unique())
    Year_list = list(groups['Year'].unique())
    for groupname in GroupName_list:
        for year in Year_list:
            populate_parameter_data(groupname, year)

if __name__ == "__main__":
    Build_Database()

    
    
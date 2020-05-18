#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: chenyuwang
"""

from Database import *
from BackTesting import *
from flask import Flask, flash, redirect, render_template, request, url_for

from socket import AF_INET, socket, SOCK_STREAM
from enum import Enum
import struct
import random
import time

import json
import sys
import queue
import threading

app = Flask(__name__)
if(len(sys.argv) > 1) :
    clientID = sys.argv[1]
else:
    clientID = "Rick"

HOST = "192.168.1.6"
PORT = 6510

BUFSIZ = 4096
ADDR = (HOST, PORT)

client_socket = socket(AF_INET, SOCK_STREAM)
client_socket.connect(ADDR)

#my_symbols = "GOOGL,FB"
group = StockGroup('Consumer Staples', 12, 2019)
q = queue.Queue()
e = threading.Event()
 
bTradeComplete = False
orders = []

class PacketTypes(Enum):
    CONNECTION_NONE = 0
    CONNECTION_REQ = 1
    CONNECTION_RSP = 2
    CLIENT_LIST_REQ = 3
    CLIENT_LIST_RSP = 4
    STOCK_LIST_REQ = 5
    STOCK_LIST_RSP = 6
    STOCK_REQ = 7
    STOCK_RSP = 8
    BOOK_INQUIRY_REQ = 9
    BOOK_INQUIRY_RSP = 10
    NEW_ORDER_REQ = 11
    NEW_ORDER_RSP = 12
    MARKET_STATUS_REQ = 13
    MARKET_STATUS_RSP = 14
    END_REQ = 15
    END_RSP = 16
    SERVER_DOWN_REQ = 17
    SERVER_DOWN_RSP = 18
    
class Packet:
    def __init__(self):
        self.m_type = 0
        self.m_msg_size = 0
        self.m_data_size = 0
        self.m_data = ""
    
    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__) + "\n"
    
    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__) + "\n"
    
    def serialize(self):
        self.m_data_size = 12 + len(self.m_data)
        self.m_msg_size = self.m_data_size
        return self.m_type.to_bytes(4, byteorder='little') + \
                self.m_msg_size.to_bytes(4, byteorder='little') + \
                self.m_data_size.to_bytes(4, byteorder='little') + \
                bytes(self.m_data, 'utf-8')
                
    def deserialize(self, message):
        msg_len = len(message)
        msg_unpack_string = '<iii' + str(msg_len-12) + 's'
        self.m_type, self.m_msg_size, self.m_data_size, msg_data = struct.unpack(msg_unpack_string, message)
        self.m_data = msg_data[0:self.m_data_size-12].decode('utf-8')
        return message[self.m_data_size:]

def receive(q=None, e=None):
    """Handles receiving of messages."""
    total_server_response = b''
    
    msgSize = 0
    while True:
        try:
            server_response = client_socket.recv(BUFSIZ)
            if len(server_response) > 0:
                total_server_response += server_response
                #print(total_server_response)
                msgSize = len(total_server_response)
                while msgSize > 0:
                    if msgSize > 12:
                        server_packet = Packet()
                        server_response = server_packet.deserialize(total_server_response)
                        #print(server_packet.m_msg_size, msgSize, server_packet.m_data)
                    if msgSize > 12 and server_packet.m_data_size <= msgSize:
                        data = json.loads(server_packet.m_data)
                        #print(type(data), data)
                        q.put([server_packet.m_type, data])
                        if e.isSet():
                            e.clear()
                        total_server_response = total_server_response[server_packet.m_data_size:]
                        msgSize = len(total_server_response)
                        server_response = b''
                    else:
                        server_response = client_socket.recv(BUFSIZ)
                        total_server_response += server_response
                        msgSize = len(total_server_response)
        #except (OSError,Exception):  
        except (KeyError,):
            q.put([PacketTypes.CONNECTION_NONE.value, Exception('receive')])
            print("Exception in receive\n")
            sys.exit(0)

def send(q=None):  
    threading.Thread(target=receive, args=(q,)).start()
    try:
        while True:
            client_packet = Packet()
            user_input = input("Action:")
            input_list = user_input.strip().split(" ")
            if len(input_list) < 2:
                print("Incorrect Input.\n")
                continue
            if "Logon" in user_input:
                client_packet.m_type = PacketTypes.CONNECTION_REQ.value
                client_packet.m_data = json.dumps({'Client':clientID, 'Status':input_list[0], 'Symbol':input_list[1]})
            
            elif "Client List" in user_input:
                client_packet.m_type = PacketTypes.CLIENT_LIST_REQ.value
                client_packet.m_data  = json.dumps({'Client':clientID, 'Status':input_list[0] + ' ' + input_list[1]})
                
            elif "Stock List" in user_input:
                client_packet.m_type = PacketTypes.STOCK_LIST_REQ.value
                client_packet.m_data  = json.dumps({'Client':clientID, 'Status':input_list[0] + ' ' + input_list[1]})
              
            elif "Book Inquiry" in user_input:
                if len(input_list) < 3:
                    print("Missing input item(s).\n")
                    continue
                client_packet.m_type = PacketTypes.BOOK_INQUIRY_REQ.value
                client_packet.m_data  = json.dumps({'Client':clientID, 'Status':input_list[0] + ' ' + input_list[1], 'Symbol':input_list[2]})
            
            elif "New Order" in user_input:
                if len(input_list) < 6:
                    print("Missing input item(s).\n")
                    continue
                client_packet.m_type = PacketTypes.NEW_ORDER_REQ.value
                client_packet.m_data  = json.dumps({'Client':clientID, 'Status':input_list[0] + ' ' + input_list[1], 'Symbol':input_list[2], 'Side':input_list[3], 'Price':input_list[4], 'Qty':input_list[5]})
            
            elif "Client Quit" in user_input:
                client_packet.m_type = PacketTypes.END_REQ.value
                client_packet.m_data = json.dumps({'Client':clientID, 'Status':input_list[0]})
            
            elif "Server Down" in user_input:
                client_packet.m_type = PacketTypes.SERVER_DOWN_REQ.value
                client_packet.m_data = json.dumps({'Client':clientID, 'Status':input_list[0] + ' ' + input_list[1]})
                
            else:
                print("Invalid message\n")
                continue
              
            client_socket.send(client_packet.serialize())
            data = json.loads(client_packet.m_data)
            #print(data)
            
            msg_type, msg_data = q.get()
            q.task_done()
            #print(msg_data)
            if msg_data is not None:
                if msg_type == PacketTypes.END_RSP.value or msg_type == PacketTypes.SERVER_DOWN_RSP.value or \
                    (msg_type == PacketTypes.CONNECTION_RSP.value and msg_data["Status"] == "Rejected"):
                    client_socket.close()
                    sys.exit(0)
        
    except(OSError, Exception):
    #except(OSError):
        q.put(PacketTypes.CONNECTION_NONE.value, Exception('send'))
        client_socket.close()
        sys.exit(0)

def logon(client_packet, symbols):
    client_packet.m_type= PacketTypes.CONNECTION_REQ.value
    client_packet.m_data = json.dumps({'Client':clientID, 'Status':'Logon', 'Symbol':symbols})
    return client_packet

def get_client_list(client_packet):
    client_packet.m_type = PacketTypes.CLIENT_LIST_REQ.value
    client_packet.m_data  = json.dumps({'Client':clientID, 'Status':'Client List'})
    return client_packet

def get_stock_list(client_packet):
    client_packet.m_type = PacketTypes.STOCK_LIST_REQ.value
    client_packet.m_data  = json.dumps({'Client':clientID, 'Status':'Stock List'})
    return client_packet

def get_market_status(client_packet):
    client_packet.m_type = PacketTypes.MARKET_STATUS_REQ.value
    client_packet.m_data = json.dumps({'Client':clientID, 'Status':'Market Status'})
    return client_packet

def get_order_book(client_packet, symbol):
    client_packet.m_type = PacketTypes.BOOK_INQUIRY_REQ.value
    client_packet.m_data = json.dumps({'Client':clientID, 'Status':'Book Inquiry', 'Symbol':symbol})
    return client_packet

def enter_a_new_order(client_packet, order_id, symbol, order_type, side, price, qty):
    if order_type == "Mkt":
        price = 0
    client_packet.m_type = PacketTypes.NEW_ORDER_REQ.value
    client_packet.m_data = json.dumps({'Client':clientID, 'OrderIndex':order_id, 'Status':'New Order', 'Symbol':symbol, 'Type':order_type, 'Side':side, 'Price':price, 'Qty':qty})
    return client_packet

def quit_connection(client_packet):
    client_packet.m_type = PacketTypes.END_REQ.value
    client_packet.m_data = json.dumps({'Client':clientID, 'Status':'Client Quit'})
    return client_packet

def send_msg(client_packet):
    client_socket.send(client_packet.serialize())
    data = json.loads(client_packet.m_data)
    print(data)
    return data


def get_response(q):
    while (q.empty() == False):
        global msg_data
        msg_type, msg_data = q.get()
        print(msg_data)
        if msg_data is not None:
            if msg_type == PacketTypes.END_RSP.value or msg_type == PacketTypes.SERVER_DOWN_RSP.value or \
                (msg_type == PacketTypes.CONNECTION_RSP.value and msg_data["Status"] == "Rejected"):
                client_socket.close()
                sys.exit(0)
    return msg_data

def set_event(e):
    e.set();

def wait_for_an_event(e):
    while e.isSet():
        continue
    
def join_trading_network(q, e):  
    global bTradeComplete
    threading.Thread(target=receive, args=(q,e)).start()
    try:
        client_packet = Packet()  
        global order_table
        set_event(e)
        global group 
        
        stock_list = group.stock_list
        my_symbols = ','.join(stock_list)
        send_msg(logon(client_packet, my_symbols))
        wait_for_an_event(e)
        
        set_event(e)
        send_msg(get_client_list(client_packet))
        wait_for_an_event(e)
        get_response(q)
    
        set_event(e)
        send_msg(get_stock_list(client_packet))
        wait_for_an_event(e)
        stock_data = get_response(q)
        
        stock_data['Stock List'] = my_symbols
        last_close = time.time()
        order_number = 0
        count = 0
        data_receive_round = 0
        position = 0
        while True:
            print("round " + str(count))
            count += 1
            
            client_packet = Packet() 
            set_event(e)
            send_msg(get_market_status(client_packet))
            wait_for_an_event(e)
            market_status_data = get_response(q)
            market_status = market_status_data["Status"]

            if (market_status == "Market Closed") or (market_status == "Not Open") or (market_status == "Pending Open"):
                if time.time() - last_close > 100:
                    break;
                time.sleep(1)
                continue
            last_close = time.time()
                
            client_packet = Packet() 
            set_event(e)
            client_msg = get_order_book(client_packet, stock_data['Stock List'])
            send_msg(client_msg)
            wait_for_an_event(e)
            data = get_response(q)
            book_data = json.loads(data)
            order_book = book_data["data"]
            trade_volume = group.trading_volume
            print("start trading")
            
            new_price = {}
            for stock in stock_list:
                new_price[stock] = [0, 0, 10000]
            # 1: middle price (best buy + best sell)/2
            # 2: best sell, maximum number of all buy orders in all buy orders
            # 3: best buy, minimum number of all sell orders in all sell orders
            for order in order_book:
                if order['Side'] == 'Buy':
                    if new_price[order['Symbol']][1] < order['Price']:
                        new_price[order['Symbol']][1] = order['Price']
                else:
                    if new_price[order['Symbol']][2] > order['Price']:
                        new_price[order['Symbol']][2] = order['Price']
            print("got best buy sell price")
            
            for stock in stock_list:
                new_price[stock][0] = round((new_price[stock][1] + new_price[stock][2])/2 , 2)
            print("compute middle price")
            
            data_receive_round += 1
            
            ### Generate Signal
            print("generate new signals")
            print(pd.DataFrame(new_price))
            for index, row in pd.DataFrame(new_price).iterrows():
                if index == 0:
                    group.createTrade(index, row)
            print("created trade")
            if data_receive_round < 2:
                time.sleep(5)
                continue
            result = group.updateTrades()
            position_new = result.iloc[-2, 4]
            
            if position_new == position:
                time.sleep(5)
                continue
            else:
                position = position_new
            if position == 0:
                time.sleep(5)
                continue
            print("position is not 0")
            OrderIndex = 0
            for stock in stock_list:
                client_packet = Packet()
                OrderIndex += 1
                client_order_id = clientID + '_' + str(OrderIndex)
                if trade_volume[stock]*position > 0:
                    position_side = 'Buy'
                    pointer = 2
                else:
                    position_side = 'Sell'
                    pointer = 1
                client_message = enter_a_new_order(client_packet, client_order_id, stock, 'Mkt', \
                              position_side, new_price[stock][pointer],abs(int(trade_volume[stock]*100000)))
                set_event(e)
                send_msg(client_message)
                wait_for_an_event(e)
                result_response = get_response(q)
                orders.append(result_response)
                while result_response['Status'] != 'Order Fill':
                    client_message = enter_a_new_order(client_packet, client_order_id, stock, 'Mkt', \
                              position_side, new_price[stock][pointer],abs(int(trade_volume[stock]*100000)))
                    set_event(e)
                    send_msg(client_message)
                    wait_for_an_event(e)
                    result_response = get_response(q)
                    orders.append(result_response)
            time.sleep(5)
            
            client_packet = Packet()
            client_msg = get_order_book(client_packet, stock_data['Stock List'])
            set_event(e)
            send_msg(client_msg)
            wait_for_an_event(e)
            get_response(q)
        print("OK, Finish all trading!")
        
        bTradeComplete = True
    
    except(OSError, Exception):
    #except(OSError):
        q.put(PacketTypes.CONNECTION_NONE.value, Exception('join_trading_network'))
        client_socket.close()
        sys.exit(0)
        

@app.route('/')
def index():
    groups = pd.read_csv(location_of_groups)
    groups = groups.transpose()
    list_of_groups = [groups[i] for i in groups]
    return render_template("index.html", group_list=list_of_groups)

@app.route('/build_model')
def build_model():
    select_st = "Select GroupName, Year, Month, Symbol, Volume from TradingQuantity;"
    result_set = execute_sql_statement(select_st, engine)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()
    result_df = result_df.transpose()
    list_of_groups = [result_df[i] for i in result_df]
    return render_template("build_model.html", group_list=list_of_groups)

@app.route('/back_test')
def model_back_testing():
    select_st = "Select GroupName, Date, Net_Value, Holding, profit_loss from Trading;"
    result_set = execute_sql_statement(select_st, engine)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()
    result_df['Holding'] = result_df['Holding'].map('{:.4f}'.format)
    result_df['profit_loss'] = result_df['profit_loss'].map('${:,.2f}'.format)
    result_df = result_df.transpose()
    list_of_groups = [result_df[i] for i in result_df]
    return render_template("back_testing.html", group_list=list_of_groups)

@app.route('/trade_analysis')
def trade_analysis():
    select_st = "SELECT GroupName, printf(\"US$%.2f\", sum(Profit_Loss)) AS Profit, \
                sum(CASE WHEN profit_loss > 0 THEN 1 ELSE 0 END) AS Profit_Trades, \
                sum(CASE WHEN profit_loss < 0 THEN 1 ELSE 0 END) AS Loss_Trades FROM Trading \
                Group BY GroupName;"
    result_set = execute_sql_statement(select_st, engine)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()
    print(result_df.to_string(index=False))
    result_df = result_df.transpose()
    trade_results = [result_df[i] for i in result_df]
    return render_template("trade_analysis.html", trade_list=trade_results)

@app.route('/start_trading')
def start_trading():
    global bClientThreadStarted
    global client_thread
    
    if bClientThreadStarted == False:
        client_thread.start()
        bClientThreadStarted = True
    
    while bTradeComplete == False:
        pass
        
    return render_template("start_trading.html", trading_results=orders)



@app.route('/trading_results', methods = ['POST', 'GET'])
def trading_result():
    """
    if request.method == 'POST':
        form_input = request.form
        client_packet = Packet()
        client_msg = enter_a_new_order(client_packet, 1, form_input['Symbol'], form_input['Side'], form_input['Price'], form_input['Quantity'])
        send_msg(client_msg)
        data = get_response(q)
    """
    result = BackTestingAnalysis("Consumer Staples", group.updateTrades())
    result_df = result.getCumulativePnL(False)
    display_result = result_df[["Holding"]]
    display_result["Net_Value"] = round(result_df["Net_Value"] * 100, 2)
    display_result["profit_loss"] = round(result_df["profit_loss"] * 100, 2)
    display_result["Cumulative_PnL"] = round(result_df["Cumulative_PnL"] * 100, 2)
    
    display_result = display_result.transpose()
    trading_results = [display_result[i] for i in display_result]
    return render_template("trading_results.html", trading_results=trading_results)
  
@app.route('/client_down')
def client_down():
    client_packet = Packet()
    msg_data = {}
    try:
        send_msg(quit_connection(client_packet))
        msg_type, msg_data = q.get()
        q.task_done()
        print(msg_data)
        return render_template("client_down.html", server_response=msg_data)
    except(OSError, Exception):
        print(msg_data)
        return render_template("client_down.html", server_response=msg_data)
    
######### 
if __name__ == "__main__":
    #build_pair_trading_model()
    #back_testing(metadata, engine, k, back_testing_start_date, back_testing_end_date)

    try:
        order_table = []
        #client_thread = threading.Thread(target=send, args=(q,))
        client_thread = threading.Thread(target=join_trading_network, args=(q,e))
        bClientThreadStarted = False
        app.run()

        if client_thread.is_alive() is True:
            client_thread.join()
            
    except (KeyError, KeyboardInterrupt, SystemExit, RuntimeError, Exception):
        client_socket.close()
        sys.exit(0)    
 

"""
        for stock in stock_list:
            print(stock)
            best_sell = order_book.loc[(order_book['Symbol'] == stock) and (order_book['Side'] == 'Sell')]['Price'].min()
            best_buy = order_book.loc[(order_book['Symbol'] == stock) and (order_book['Side'] == 'Buy')]['Price'].max()
            new_price[stock] = [(best_sell + best_buy)/2, best_sell, best_buy]
        
        stock_new_price = pd.DataFrame([new_price[x][0] for x in stock_list], index = stock_list).T
        
        print('Get new stock price')
        print(stock_new_price)
        group.createTrade(0, stock_new_price)
        print('create new trade')
        print(group.trades)
        if group.trades['Date'] >= 1:
            result = group.update_trades()
            position = result.iloc[-1, 4]
            trade_volume = group.trading_volume
            if position == 1:
                side = 'Buy'
                for stock in stock_list:
                    client_order_id = clientID + '_' + str(OrderIndex)
                    OrderIndex += 1
                    client_message = enter_a_new_order(client_packet, client_order_id, stock, side, new_price[stock][2],int(trade_volume[stock]*100))
                    set_event(e)
                    send_msg(client_message)
                    wait_for_an_event(e)
                    orders.append(get_response(q))
            elif position == -1:
                side = 'Sell'
                for stock in stock_list:
                    client_order_id = clientID + '_' + str(OrderIndex)
                    OrderIndex += 1
                    client_message = enter_a_new_order(client_packet, client_order_id, stock, side, new_price[stock][1],int(trade_volume[stock]*100))
                    set_event(e)
                    send_msg(client_message)
                    wait_for_an_event(e)
                    orders.append(get_response(q))
        
        OrderIndex = 0
        for order in order_book:
            client_packet = Packet()
            OrderIndex += 1
            client_order_id = clientID + '_' + str(OrderIndex)
            client_message = enter_a_new_order(client_packet, client_order_id, order['Symbol'], 'Lmt' if random.randint(1,100) % 2 == 0 else 'Mkt', \
                              'Buy' if order['Side'] == 'Sell' else 'Sell', order['Price'], int(order['OrigQty']*2))
            
            set_event(e)
            send_msg(client_message)
            wait_for_an_event(e)
            orders.append(get_response(q))
        """      
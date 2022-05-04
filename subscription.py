#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 12:34:16 2022

@author: q

Kline/Candlestick Streams

    Payload:

{
  "e": "kline",     // Event type
  "E": 123456789,   // Event time
  "s": "BNBBTC",    // Symbol
  "k": {
    "t": 123400000, // Kline start time
    "T": 123460000, // Kline close time
    "s": "BNBBTC",  // Symbol
    "i": "1m",      // Interval
    "f": 100,       // First trade ID
    "L": 200,       // Last trade ID
    "o": "0.0010",  // Open price
    "c": "0.0020",  // Close price
    "h": "0.0025",  // High price
    "l": "0.0015",  // Low price
    "v": "1000",    // Base asset volume
    "n": 100,       // Number of trades
    "x": false,     // Is this kline closed?
    "q": "1.0000",  // Quote asset volume
    "V": "500",     // Taker buy base asset volume
    "Q": "0.500",   // Taker buy quote asset volume
    "B": "123456"   // Ignore
  }
}

The Kline/Candlestick Stream push updates to the current klines/candlestick every second.

"""

import websocket
import json
import time
import datetime
from collections import deque
import pandas as pd
from threading import Thread

UTC = datetime.timezone(offset = datetime.timedelta(0)) 
wss = 'wss://stream.binance.com:9443/ws/'

class Websocket():
    
    def __init__(self, socket : str):
        self.socket = socket
        self.log = {}
        pass
    
    def log_event(self, event):
        timestamp = str(datetime.datetime.timestamp(datetime.datetime.now().astimezone(UTC)))
        self.log[timestamp] = event
        pass

    def get_log(self):
        return pd.DataFrame(pd.Series(self.log), columns = ['message'])
    
    def on_message(self, ws, message):
        # store close prices
        print(message, type(message))
        pass
    
    
    def on_error(self, ws, error):
        self.log_event(error)
        pass

    def on_close(self, ws):
        pass

    def stream(self, currency, interval):
        websocket.enableTrace(traceable = True)
        kline = f'{currency}@kline_{interval}'
        self.ws = websocket.WebSocketApp(self.socket + kline,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close,
                                         )
        Thread(target = self.ws.run_forever).start()
        pass
    
    pass


class TradingSocket(Websocket):

    def __init__(self, socket : str):
        super().__init__(socket = socket)
        self.memory = deque(maxlen=100)
        pass
    
    def cast_klines(self, message):
        """ Cast to ohlcv """
        message = json.loads(message)['k']

        bar = {'Open' : float(message["o"]), 
               'High' : float(message["h"]),
               'Low' : float(message["l"]),
               'Close' : float(message["c"]), 
               'Volume' : float(message["q"]), 
               }
        
        return bar
    
    def on_message(self, ws, message):
        # store close prices
        self.memory.append(self.cast_klines(message))
        if len(self.memory) > 11:
            self.log_event('Computing TA & Signal')
            self.log_event('Checking Position')
            self.log_event('Triggering Order')
        pass
    
    def get_memory(self):
        return self.memory
    

def test_Websocket():
    
    ws = Websocket(socket = wss)
    
    ws.stream('btcusdt', '1m')

    pass

def test_TradingSocket():
    
    td = TradingSocket(socket = wss)
    
    td.stream('ethusdt', '1m')
    
    time.sleep(45)
    
    pd.DataFrame(td.get_memory())['Close'].plot(grid = True)
    
    pass
   
    
if __name__ == '__main__':
    

    # test_Websocket()
    
    td = TradingSocket(socket = wss)
    
    td.stream('btcusdt', '1m')
    
    pd.DataFrame(td.get_memory())['Close'].plot(grid = True)
    
    
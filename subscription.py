#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 12:34:16 2022

@author: q
"""

import websocket
import datetime
from collections import deque
import pandas as pd


UTC = datetime.timezone(offset = datetime.timedelta(0)) 
wss = 'wss://stream.binance.us:9443'

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
        self.memory.append(message)
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
        self.ws.run_forever()
        pass
    
    pass


class TradingSocket(Websocket):

    def __init__(self, socket : str):
        super().__init__(socket = socket)
        self.memory = deque(maxlen=100)
        pass

    def on_message(self, ws, message):
        # store close prices
        self.memory.append(message)
        if len(self.memory) > 11:
            self.log_event('Computing TA & Signal')
            self.log_event('Checking Position')
            self.log_event('Triggering Order')
        pass
    

    
if __name__ == '__main__':
    
    ws = Websocket(socket = wss)
    
    ws.stream('btcusdt', '1m')
    
    print(ws.get_log())

    td = TradingSocket(socket = wss)
    
    
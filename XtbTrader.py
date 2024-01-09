import pandas as pd
from xAPIConnector import *
import time
from datetime import timedelta
import datetime
import config
#import talib as ta

class XtbTrader():
    
    def __init__(self, client, ssid, instrument, interval, lookback, strategy, units, end):
        
        '''
        
        Description
        ===============================================
        Python module let you trade with your strategy wit XTB broker
        
        :argument
        *client -> XTB API client
        *ssid -> stream session id required to start streaming candles
        *instrument -> string - instrument you want to trade on e.g. 'EURUSD'
        *interval -> string - trading interval, {'1min', '5min', '15min', '30min', '1h', '4h', '1D'}
        *lookback -> int - how many candles look back to calculate your strategy indicator e.g. 1000
        *strategy -> name of defined strategy in python,
        *units -> float number of units of given instrument
        *end -> str, date time when trading session shoud be terminated in foramt 'YYYY-mm-dd HH:MM'


        
        
        '''
        
        self.instrument=instrument
        self.interval=interval
        self.lookback=lookback
        self.strategy=strategy
        self.units=units
        self.end=end
        self.interval_dict = {'1min': 1, '5min': 5, '15min': 15, '30min': 30, '1h': 60, '4h': 240, '1D': 1440}
        self.collect_live=False
        self.live_df=pd.DataFrame()
        self.strategy=strategy
        self.position_closed=False
        self.check_hist=True
        self.session_history=[]
        self.session_hist_df=pd.DataFrame()
        self.trade_counter=0
        self.client = client
        self.cmd_dict={0:'Buy',1:'Sell',2:'Buy_limit',3:'Sell_limit', 4:'Buy_stop', 5:'Sell_stop'}
        self.session_started=False
        self.get_last_n_candles()
        self.session_start=datetime.datetime.now()
        self.session_end=datetime.datetime.strptime(end,'%Y-%m-%d %H:%M')
        self.sclient = APIStreamClient(ssId=ssid, candleFun=self.procCandle)
        self.sclient.subscribeCandle(self.instrument)
        self.terminate_session=False
        start_session_date=self.session_start.strftime("%a,%d %b, %Y, %H:%M:%S")
        print(f"START SESSION AT {start_session_date} ")
        with open('valid_xtb_symbols.json') as f:
            
            valid_symbols_dict=json.load(f)
        
        self.valid_symbols=valid_symbols_dict['symbols']
        
        if self.instrument not in self.valid_symbols:
            raise InvalidSymbol(self.instrument, self.valid_symbols)
            
        if self.interval not in list(self.interval_dict.keys()):
            
            raise InvalidInterval(self.interval)

        
            
    def __repr__(self):
        
        return f'XtbTrader || instrument={self.instrument}; interval={self.interval}; strategy={self.strategy}; units={self.units}; end of session={self.end}'
    
    
    def history_converter(self, history):
        
        '''
        Description
        ===============================================
        Private method converting history of prices from json format to pandas df
        
        :param
        history -> json - json data with history for given instrument - result of method get_last_n_candles
        
        :return
        pandas df
        '''
        df_dict = history['returnData']['rateInfos']
        digits = history['returnData']['digits']

        df = pd.DataFrame.from_dict(df_dict)

        df['time'] = df['ctm'].apply(lambda x: datetime.datetime.fromtimestamp(x / 1000))
        df['open'] = df['open'] / (10 ** digits)
        df['close'] = df['open'] + df['close'] / (10 ** digits)
        df['high'] = df['open'] + df['high'] / (10 ** digits)
        df['low'] = df['open'] + df['low'] / (10 ** digits)
        df = df[['time','open','high','low', 'close', 'vol']]
        df.set_index("time", inplace=True, drop=True)
        df=self.strategy(df)
        
        return df

    def get_last_n_candles(self):
        
        
        '''
        Description
        ===============================================
        Method returns last n candles from given interval

        '''
        
        
        
        args = {
            'info': {
                'period': self.interval_dict[self.interval],
                'ticks': self.lookback * (-1),
                'symbol': self.instrument,
                'start': self.client.commandExecute('getServerTime')['returnData']['time']
            }}

        data = self.client.commandExecute('getChartRangeRequest', arguments=args)

        df = self.history_converter(data)
        
        if len(df)!=self.lookback:
            print(f'For {self.interval} interval you can get last {len(df)} candles now, for more info about limitations visit http://developers.xstore.pro/documentation/#getChartRangeRequest')


        self.raw_data=df.copy()
        
        
        self.last_bar = self.raw_data.index[-1]
    
    def procCandle(self, msg):
        
        '''
        Description
        ===============================================
        Method processing message from XTB API to pandas df format
        
        :param
        msg -> message from XTB server
        
        :return

        pandas df
        
        '''
        
        
        if self.session_end!=None:
            close_session=False

            if self.session_end<=datetime.datetime.now():

                 close_session=True
            if close_session:

                 self.terminate_session = True
                 self.sclient.unsubscribeCandle(self.instrument)
                 self.close_order()
                 self.getTradeHistory(self.order_hist)

                 end_session=datetime.datetime.now().strftime("%a,%d %b, %Y, %H:%M:%S")
                 print(f'TRADING SESSION END AT {end_session}')
                 if len(self.session_history)>0:
                    print(f"Profit generated during trading session: {self.session_hist_df['cum_profit'].iloc[-1]}")
                 print(f'=================================================')
                 time.sleep(5)


		
        if self.terminate_session==False:
            record = pd.DataFrame.from_records(msg['data'], index=[pd.to_datetime(datetime.datetime.fromtimestamp(
                msg['data']['ctm'] / 1000))])
            record=record[['open','high','low','close','vol']]
            #recent_candle = pd.to_datetime(datetime.datetime.fromtimestamp(msg['data']['ctm']/1000))
            self.last_candle=record.resample('1min', label='right').last().index[-1]


            if (self.last_candle-self.last_bar>pd.to_timedelta(self.interval)) & (self.collect_live==False):

                if self.interval!='1min':
                    self.get_last_n_candles()


                self.collect_live=True
                print('START COLLECTING LIVE DATA')
                print('Waiting for first trade signal...')
                print('=================================')


            if self.collect_live==True:


                if self.interval!='1min':

                    self.live_df=pd.concat([self.live_df, record])

                    if self.last_candle-self.live_df.index[0]==pd.to_timedelta(self.interval):


                        self.live_df=self.live_df.resample(self.interval).agg({"open": "first", "high": "max", "low": "min", "close": "last", "vol":"sum"}).dropna()
                        self.raw_data=pd.concat([self.raw_data, self.live_df])
                        self.live_df=pd.DataFrame()
                        self.raw_data=self.strategy(self.raw_data)
                        self.trade()
                else:

                    self.raw_data=pd.concat([self.raw_data, record])
                    self.raw_data=self.strategy(self.raw_data)
                    self.trade()


    def trade(self):

        '''

        Description
        ===============================================
        Method creating new order and closing orders depends on current position and sreategy
        '''
        
        df=self.raw_data.copy()
        
        if df['position'].iloc[-2]==1:
            if df['position'].iloc[-1]==-1:
                
                self.close_order()
                self.getTradeHistory(self.order_hist)
                if self.position_closed:
                    self.open_position('sell')
            if df['position'].iloc[-1]==0:
                
                self.close_order()
                self.getTradeHistory(self.order_hist)
                


        if df['position'].iloc[-2]==-1:
            if df['position'].iloc[-1]==1:
                
                self.close_order()
                self.getTradeHistory(self.order_hist)
                if self.position_closed:
                    self.open_position('buy')
            if df['position'].iloc[-1]==0:

                self.close_order()
                self.getTradeHistory(self.order_hist)
                
        if df['position'].iloc[-2]==0:
            if df['position'].iloc[-1]==1:
                
               self.open_position('buy')
                    
                
            if df['position'].iloc[-1]==-1:
                
               self.open_position('sell')
                    
                

        
            
            
    def close_order(self):
        '''
        Description
        ===============================================
        Method that close active trading positions

        '''
      
        order_to_close = self.client.commandExecute("getTrades", arguments={'openedOnly': True})
        
        if len(order_to_close['returnData']) > 0:
            
            order_close = self.client.commandExecute("tradeTransaction", arguments={
                'tradeTransInfo': {
                    'order': order_to_close['returnData'][-1]['order'],
                    'type': TransactionType.ORDER_CLOSE,
                    'volume': order_to_close['returnData'][-1]['volume'],
                    'symbol': order_to_close['returnData'][-1]['symbol'],
                    'price': order_to_close['returnData'][-1]['close_price'],

                }

            })
            
            self.check_order_closed(order_to_close)
                
               
                    
        else:
            
            self.order_hist=None
            self.check_hist=False
            self.position_closed=True
        
        
    def open_position(self, position_type):

        '''
        Description
        ===============================================
        Method opening new trading position
        :param

        *position_type-> str {'buy','sell'}

        '''
        
        positions_dict={'sell':TransactionSide.SELL, 'buy':TransactionSide.BUY}
        ask_bid_dict={'sell':'bid','buy':'ask'}
        
        
        order_type=positions_dict[position_type]
        ask_bid=ask_bid_dict[position_type]
        
    
        order = self.client.commandExecute("tradeTransaction", arguments={
                    'tradeTransInfo': {
                        'cmd': order_type,
                        'volume': self.units,
                        'symbol': self.instrument,
                        'price': self.client.commandExecute("getSymbol", arguments={'symbol': self.instrument})['returnData'][ask_bid]

                    }

                })
        
        
        order_num=order['returnData']['order']
        
        
        
        self.check_order(order_num)
        
        
        if self.trade_counter==1:
            start_session=datetime.datetime.now().strftime("%a,%d %b, %Y, %H:%M:%S")
            print(f'START TRADING AT {start_session} |instrument: {self.instrument}| strategy: {self.strategy.__name__} | interval: {self.interval}')
            self.session_started=True
        
       
        self.check_opened_position(self.current_order_num)
        
        
    
                
    def getTradeHistory(self,order):

        '''
        Description
        ===============================================
        Method summarizing closed possition and whole trading session
        :param

        *order-> json, last closed order

        '''
        
        if (self.check_hist) & (self.order_hist!=None):
            

            hist_record=None
            order_num=order['order']
            
            
            retry=0
            while True:
                order_history=self.client.commandExecute("getTradesHistory", arguments={'end': 0, 'start': 0})
                time.sleep(0.5)
                
                retry+=1
                if retry>15:
                    break

                if type(order_history)==dict:
                    if 'status' in order_history.keys():
                        if order_history['status']==True:
                            found_hist_order=False
                            for d in order_history['returnData']:
                                if order_num == d['position']:
                                    found_hist_order=True
                                    print(f'Trade {order_num} saved in trades history')
                                    hist_record=d
                                    self.session_history.append(hist_record)
                                    self.session_hist_df=pd.DataFrame.from_dict(self.session_history)
                                    self.session_hist_df['cum_profit']=self.session_hist_df['profit'].cumsum()
                                    print(f'Closed trade {order_num} details:')
                                    print(f"Open price: {hist_record['open_price']}")
                                    print(f"Close price: {hist_record['close_price']}")
                                    print(f"Trade profit: {hist_record['profit']}")
                                    if 'cum_profit' in self.session_hist_df.columns:
                                        print(f"Cumulative profit for whole trading session: {self.session_hist_df['cum_profit'].iloc[-1]}")
                                    print(f'Number of trades in session: {self.trade_counter}')
                                    print('=======================================================')
                                    break      
                            
                            if found_hist_order==True:
                                break
                            
            
            if hist_record==None:
                
                print(f'Order {order_num} not found in history')
                print('==========================================')
                pass

        else:
            if (self.terminate_session==True) & (len(self.session_history)==0):
                print('No trades in current session')
                pass

            else:
                pass
        
    
    
        
                    
        
    def check_opened_position(self, current_order):

        '''
        Description
        ===============================================
        Method confirming that order that was sent is opened

        :param
        *current_order-> int, order that was sent to broker

        '''
        retry=0
        while True:
            trade_details=self.client.commandExecute('getTrades', arguments={"openedOnly":True})
            time.sleep(0.5)
            retry+= 1
            if retry > 15:
                print(f'Not found order {current_order}')
                break
            
            if type(trade_details)==dict:
                if 'status' in trade_details.keys():
                    if trade_details['status']==True:
                        if len(trade_details['returnData'])>0:
                            if current_order==trade_details['returnData'][-1]['order2']:
                                
                                open_price=trade_details['returnData'][-1]['open_price']
                                position=self.cmd_dict[trade_details['returnData'][-1]['cmd']]
                                trade_position=trade_details['returnData'][-1]['position']
                                open_time=datetime.datetime.now().strftime("%a,%d %b, %Y, %H:%M:%S")

                                print(f'{position} order number {current_order} with position {trade_position} at {open_time} with open price {open_price} accepted')
                                print('-----------------------------------------------------------------------------------------------------------------')
        
                        
                                break
                    else:
                        print(trade_details)
                        break
        
        
    
    def check_order(self, order_num):

        '''
        Description
        ===============================================
        Method check order status

        :param
        *order_num-> int, order that was sent to broker

        '''
        retry=0
        while True:
            
            check_order=self.client.commandExecute("tradeTransactionStatus", arguments={'order': order_num})
            time.sleep(0.5)
            retry+= 1
            if retry > 15:
                print('Cannot specify transaction status')
                break
            if type(check_order)==dict:
                if 'status' in check_order.keys():
                    if check_order['status']==True:
                        order_status=check_order['returnData']['requestStatus']
                        if order_status==3:

                            self.trade_counter=self.trade_counter+1
                            self.current_order_num=check_order['returnData']['order']

                            break
                        if order_status==4:
                            print(f"Order {order_num} has been rejected")
                            self.current_order_num=0
                            break
                        
                        if order_status==1:
                            
                            print(f"Order {order_num} has error")
                            self.current_order_num=0
                            break
                    else:
                        print(check_order)
                        break
                        
    def check_order_closed(self, order_to_close):

        '''
        Description
        ===============================================
        Method check if position is closed

        :param
        *order_to_close-> json, order that was sent to close

        '''
        
        retry=0
        while True:
            check_order=self.client.commandExecute("getTrades", arguments={'openedOnly': True})
            time.sleep(0.5)
            retry+= 1
            if retry > 15:
                print('Cannot specify if order is closed')
                break
            
            if type(check_order)==dict:
                if 'status' in check_order.keys():
                    if check_order['status']==True:
                        self.check_hist=True
                        self.order_hist=order_to_close['returnData'][-1]
                        print(f"Trade {self.order_hist['order']} closed")
                        self.position_closed=True
                        break
                        
                    else:
                        print(check_order)
                        break
               
class InvalidInterval(Exception):
    """Exception raised when invalid trading interval occured.

    Attributes:
        interval -- interval which caused the error
        message -- explanation of the error
    """

    def __init__(self, interval, message="Choose from one of possible intervals of trading {'1min', '5min', '15min', '30min', '1h', '4h', '1D'}"):
        self.interval = interval
        self.message = message
        super().__init__(self.message)
        

class InvalidSymbol(Exception):
    """Exception raised when invalid trading symbol occured.

    Attributes:
        symbol -- symbol which caused the error
        valid_symbols -- list of valid symbols
        message -- explanation of the error
    """

    def __init__(self, symbol, valid_symbols):
        self.valid_symbols=valid_symbols
        self.symbol= symbol
        self.message = f"Symbol {self.symbol} is invalid, choose from one of possible instruments:  {self.valid_symbols}"
        super().__init__(self.message)

    

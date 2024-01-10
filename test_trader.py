from XtbTrader import *
from xAPIConnector import *
import numpy as np
from config import *

def contrarian(df, window=1):
    df["returns"] = np.log(df['close'] / df['close'].shift(1))
    df["position"] = -np.sign(df["returns"].rolling(window).mean())
    return df

if __name__ == '__main__':
    client = APIClient()
    resp=client.execute(loginCommand(user_id, pwd))
    ssid=resp['streamSessionId']
    xt=XtbTrader(client=client, ssid=ssid, instrument='EURUSD', interval='5min', lookback=1000, strategy=contrarian, units=0.1, end='<date in format YYYY-mm-dd HH:MM>', csv_results_path='<PATH_TO_SAVE_CSV_RESULTS>')
    while True:

        if xt.terminate_session:
            time.sleep(5)
            break

    xt.sclient.disconnect()
    client.disconnect()



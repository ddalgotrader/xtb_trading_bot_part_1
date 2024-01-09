from xAPIConnector import *
import config

user_id = config.user_id
pwd= config.pwd

client=APIClient()
resp=client.execute(loginCommand(user_id, pwd))
ssid=resp['streamSessionId']

sclient=APIStreamClient(ssId=ssid, tickFun=procTickExample)
sclient.subscribePrice('EURUSD')

time.sleep(5)

sclient.unsubscribePrice('EURUSD')
time.sleep(5)
sclient.disconnect()
client.disconnect()

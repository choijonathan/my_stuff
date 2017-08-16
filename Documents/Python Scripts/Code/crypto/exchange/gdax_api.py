import data_config as config
import requests
import datetime
import time
import pandas as pd

class GDAX():
    def __init__(self):
        self.base_url =r'https://api.gdax.com/'

    def get_historical_rates(self,product,start,end=None,
                             granularity=60):
        if end==None:
            end=_end_time_for_given_granularity(start,granularity)
        url=self.base_url+r"/products/%s/candles"%(product)
        my_request=requests.get(url,params={'start':start.isoformat(),
                                            'end':end,
                                            'granularity':granularity})

        requested_data=pd.DataFrame(my_request.json(),columns=['Date', 'Low', 'High', 'Open', 'Close', 'Volume'])
        requested_data['Date'] = requested_data['Date'].apply(lambda x: datetime.datetime.utcfromtimestamp(x))
        requested_data = requested_data.set_index('Date').tz_localize('UTC')
        return requested_data.sort_index()


    def get_historical_rate_for_range(self,product,start,end,granularity):
        curr_date=start
        df=pd.DataFrame({},columns=['Low','High','Open','Close','Volume'])
        while curr_date<end:
            print curr_date
            curr_pull=self.get_historical_rates(product,curr_date,granularity=granularity)
            df=df.append(curr_pull)
            curr_date=_next_interval_start(curr_date,granularity)
            time.sleep(1)
        return df


def _end_time_for_given_granularity(start_datetime, granularity=60):
    """ for a given datetime start, will calculate the correct end time to stay within 400 candles
    for that particular level of granularity"""
    number_of_seconds_in_interval = granularity * 400
    end_datetime = start_datetime + datetime.timedelta(0, number_of_seconds_in_interval)
    return end_datetime

def _next_interval_start(start_datetime,granularity=60):
    return _end_time_for_given_granularity(start_datetime,granularity=granularity)+datetime.timedelta(0,granularity)
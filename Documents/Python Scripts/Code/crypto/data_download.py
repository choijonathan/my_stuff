import os
import time
import pandas as pd
import datetime
FETCH_URL = "https://poloniex.com/public?command=returnChartData&currencyPair=%s&start=%d&end=%d&period=300"
COLUMNS = ["date","high","low","open","close","volume","quoteVolume","weightedAverage"]
DATA_PATH=r"C:\Users\jonathanchoi\Documents\Python Scripts\data"

def download_pair(pair):
    datafile = os.path.join(DATA_PATH, pair + ".csv")
    timefile = os.path.join(DATA_PATH, pair)

    if os.path.exists(datafile):
        newfile = False
        start_time = int(open(timefile).readline()) + 1
    else:
        newfile = True
        start_time = 1388534400  # 2014.01.01
    end_time = 9999999999  # start_time + 86400*30

    url = FETCH_URL % (pair, start_time, end_time)
    print("Get %s from %d to %d" % (pair, start_time, end_time))

    df = pd.read_json(url, convert_dates=False)

    if df["date"].iloc[-1] == 0:
        print("No data.")
        return

    end_time = df["date"].iloc[-1]
    ft = open(timefile, "w")
    ft.write("%d\n" % end_time)
    ft.close()
    outf = open(datafile, "a")
    if newfile:
        df.to_csv(outf, index=False, columns=COLUMNS)
    else:
        df.to_csv(outf, index=False, columns=COLUMNS, header=False)
    outf.close()
    print("Finish.")
    time.sleep(30)

def get_pair_list():
    df = pd.read_json("https://poloniex.com/public?command=return24hVolume")

    return [x for x in df.columns if '_' in x ]

def load_pair(pair):
    path=os.path.join(DATA_PATH,pair.upper()+'.csv')
    df=pd.read_csv(path)
    df['date']=df['date'].apply(lambda x:datetime.datetime.utcfromtimestamp(x))
    df=df.set_index('date').tz_localize('UTC')
    returns = df['weightedAverage'] / df['weightedAverage'].shift(1) - 1
    df['cumProd']=(1+returns).cumprod()
    df['cumProd'][0]=1
    return df

def load_all_assets():
    file_list=os.listdir(DATA_PATH)
    csv_list = [x for x in file_list if '.csv' in x]
    out={}
    for pair in csv_list:
        print pair
        pair_name=pair.split('.csv')[0]
        out[pair_name]=load_pair(pair_name)
    return pd.Panel(out)


def zscore(df):
    mean = pd.rolling_mean(df, 12 * 24 * 360, min_periods=12 * 24 * 30)
    std = pd.rolling_std(df, 12 * 24 * 360, min_periods=12 * 24 * 30)
    return (df - mean) / std


def timing_curve(df):
    return (df).clip(-4, 4) / 4

def reversal_pl(btc_ret,long_only=True):
    model=pd.ols(y=btc_ret,x=btc_ret.shift(1),window=12*24*5,window_type='rolling')
    betas=model.beta['x']
    signal=timing_curve(betas*zscore(btc_ret))
    sigma=np.sqrt(365*24*12)*pd.ewmstd(btc_ret,com=12*24*5,min_periods=12*24*5)
    if long_only:
        view=(.3*(signal)/sigma).clip(0,9999999)
    else:
        view=(.3*(signal)/sigma)
    t_cost=(view.diff()).abs()*.005
    pl=view.shift(1)*btc_ret
    net_pl=view.shift(1)*(btc_ret)-t_cost
    turnover=365*12*24*view.diff().abs().mean()
    print calc_sharpe(pl)
    print calc_sharpe(net_pl)
    print turnover
    return pd.DataFrame({'view':view,
           'pl':pl,
           'net_pl':net_pl})
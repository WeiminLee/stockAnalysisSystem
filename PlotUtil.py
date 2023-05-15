# -*- encoding: utf-8 -*-
# user:LWM
import pandas as pd
from common.PlotTemplete import PlotTemplete

def format_stock_data_to_kst(df):
    df['date'] = pd.to_datetime(df['date'])
    df.rename(columns={"date": "time", 'vol': "volume"}, inplace=True)
    df.index = df['time']
    return df


def plot_candle_stick(df, stock_name):
    df_arr = format_stock_data_to_kst(df)
    mpf.plot(df_arr,
             style=PlotTemplete.style,
             type='candle',
             title=stock_name,
             volume=True,
             mav=(5, 10, 20),
             show_nontrading=False
             )  # 绘制k线图

def plot_munite_data(df):
    plt.plot(df['close'].values)
    plt.show()
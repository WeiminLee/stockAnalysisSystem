# -*- encoding: utf-8 -*-
# user:LWM

import akshare as ak
from tqdm import tqdm
import DataUtil, DateUtil, SqlUtil
from common.Const import Const
import warnings
from pandas.core.common import SettingWithCopyWarning

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

"""
使用 efinance接口 完成股票基本数据的更新，存储于stock_data 数据库表
"""


def update_daily_stock_data():
    all_code = SqlUtil.get_all_sql_exist_stock_code()
    print(len(all_code))
    end = DateUtil.get_today()
    step = 100
    for i in tqdm(range(0, len(all_code), step)):
        codes = all_code[i:i + step]
        sorted_dates = SqlUtil.get_lasted_date_by_codes(codes)
        start_date = sorted_dates[-1]
        end_date = sorted_dates[0]
        print("起始日期为：",sorted_dates[-1],"终止日期为：",sorted_dates[0])
        if start_date > '20190101':
            beg = '20190101'
        else:
            beg = end_date
        # 获取数据库中的数据
        df1 = SqlUtil.get_stock_data_batch_from_sql(codes, beg=DateUtil.get_n_days_before_from_beg(end, 30), end=end)
        df2_dict = DataUtil.get_stock_data_bar(codes, beg, end.replace('-', ''))
        for code, df3 in df2_dict.items():
            exist_dates = df1[df1['code'] == code]['date'].values.tolist()
            df3.rename(columns=Const.rename_dict, inplace=True)
            df3 = df3[~df3['date'].isin(exist_dates)]
            df3.sort_values(by=['date'], ascending=False, inplace=True)
            df3.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
            SqlUtil.insert_sql(df3, 'stock_data')


def update_zt_stock_data():
    df = SqlUtil.get_stock_data_all(code='000001')
    dates = df['date'].values.tolist()
    exist_dates = set(SqlUtil.get_exist_zt_dates())
    for date in tqdm(dates):
        if date in exist_dates:
            continue
        print('日期：',date)
        df = ak.stock_zt_pool_em(date=date.replace('-', ''))
        if df.empty:
            print(date,'数据为空')
            continue
        df.rename(columns=Const.rename_zt, inplace=True)
        df.drop(['index'], axis=1, inplace=True)
        df['date'] = date
        SqlUtil.insert_sql(df, 'stock_zt_data')


if __name__ == '__main__':
    update_daily_stock_data()
    update_zt_stock_data()

# -*- encoding: utf-8 -*-
# user:LWM
"""
更新大盘行情数据
"""
import SqlUtil
import akshare as ak
from tqdm import tqdm


def update_dapan_data():
    codes = ['sh000001', 'sh000016', 'sh000300', 'sh000905', 'sz399001', 'sz399005', 'sz399006']
    for code in tqdm(codes):
        df = ak.stock_zh_index_daily(symbol=code)
        df['code'] = code
        # 获取数据库中存在的数据
        dapan_df = SqlUtil.get_single_dapan_data_all(code)
        dapan_df['date'] = dapan_df['date'].apply(lambda x:str(x))
        exist_date = list(set(dapan_df['date'].values.tolist()))
        # 去除重复
        df = df[~df['date'].isin(exist_date)]
        SqlUtil.insert_sql(df, 'dapan_data')


if __name__ == '__main__':
    update_dapan_data()

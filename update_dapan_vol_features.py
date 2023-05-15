# -*- encoding: utf-8 -*-
# user:LWM
"""
更新大盘成交量特征，同时更新板块成交量占比信息
"""
import collections
import os

import DataUtil
import DateUtil
import SqlUtil
from tqdm import tqdm
import pandas as pd
import Utils


def get_dapan_col_dict_info():
    df1 = pd.read_csv('../StaticsData/index_data/SZCZ.csv')
    df3 = pd.read_csv('../StaticsData/index_data/SZZS.csv')
    df1['成交额'] = df1['成交额'].apply(lambda x: x / 100000000)
    df3['成交额'] = df3['成交额'].apply(lambda x: x / 100000000)
    amount = df1['成交额'] + df3['成交额']
    df = df1[['日期', '开盘', '收盘', '最高', '最低']].copy()
    df.rename(columns={'日期': "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low"}, inplace=True)
    df['amount'] = amount
    df.index = df['date'].apply(lambda x: x.replace("/", "-"))
    dapan_dict = df['amount'].to_dict()
    return dapan_dict


def get_concept_dapan_vol_ratio(valid_dates, dapan_dict):
    result = collections.defaultdict(list)
    for date in valid_dates:
        dapan_vol = dapan_dict[date]
        for con, item in sacs[date].items():
            result['date'].append(date)
            result['concept'].append(con)
            result['total'].append(item['total'])
            result['dapan'].append(round(dapan_vol, 3))
            result['rate'].append(round(item['total'] / dapan_vol, 6))
    return result


def update_szzc_szzs_bar_data():
    szcz_file_name = r'D:\LearningProgram\trendStrategySystem\StaticsData\index_data\SZCZ.csv'
    szzs_file_name = r'D:\LearningProgram\trendStrategySystem\StaticsData\index_data\SZZS.csv'

    szcz_df, szzs_df = pd.DataFrame(), pd.DataFrame()
    if os.path.exists(szcz_file_name):
        szcz_df = pd.read_csv(szcz_file_name, encoding='utf-8')
    if os.path.exists(szzs_file_name):
        szzs_df = pd.read_csv(szzs_file_name, encoding='utf-8')
    beg = '20190101'
    if not szzs_df.empty:
        beg = max(szzs_df['日期'].values).replace("-","")
    print('起始日期为：',beg)
    df = DataUtil.get_stock_data_bar(codes=['399001', 'SZZS'], beg=beg)
    szzs_df = pd.concat([szzs_df, df['SZZS']])
    szzs_df.drop_duplicates(subset=['日期'], keep='last', inplace=True)
    szzs_df.sort_values(by=['日期'], ascending=True, inplace=True)

    szcz_df = pd.concat([szcz_df, df['399001']])
    szcz_df.drop_duplicates(subset=['日期'], keep='last', inplace=True)
    szcz_df.sort_values(by=['日期'], ascending=True, inplace=True)
    szcz_df.to_csv(szcz_file_name, encoding='utf-8', index=False)
    szzs_df.to_csv(szzs_file_name, encoding='utf-8', index=False)


if __name__ == '__main__':
    update_szzc_szzs_bar_data()
    sacs_file_name = r'D:\LearningProgram\trendStrategySystem\StaticsData\sacs.json'
    save_dir = r'D:\LearningProgram\trendStrategySystem\StaticsData\open_features'
    dapan_dict = get_dapan_col_dict_info()
    sacs = Utils.read_json(sacs_file_name)
    concept_vol_feature_file_name = '../StaticsData/concept_features/concept_dapan_vol_ratio_features.csv'
    df = pd.read_csv(concept_vol_feature_file_name, dtype={'date': str})
    dates = df['date'].values.tolist()
    valid_dates = [x for x in sacs.keys() if x not in dates]
    print('剩余更新时间：', valid_dates)
    result = get_concept_dapan_vol_ratio(valid_dates, dapan_dict)
    df = pd.concat([df, pd.DataFrame(result)])
    df.drop_duplicates(subset=['date', 'concept'], keep='last', inplace=True)
    df.to_csv(concept_vol_feature_file_name, index=False)

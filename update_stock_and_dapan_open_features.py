# -*- encoding: utf-8 -*-
# user:LWM
import collections
import os.path
import pandas as pd
from tqdm import tqdm
import SqlUtil, DataUtil, Utils

"""
计算个股开盘特征数据
1、开盘价特征
2、开盘时大盘特征
3、开盘时板块特征
"""

sacs_file_name = r'D:\LearningProgram\trendStrategySystem\StaticsData\sacs.json'
sacs = Utils.read_json(sacs_file_name)
sacs_dates = [x for x in list(sacs.keys()) if x >= '2020-01-01']

dapan_codes = ['sh000001', 'sh000016', 'sh000300', 'sh000905', 'sz399001', 'sz399005', 'sz399006']


def get_stock_and_dapan_open_features(df):
    # 获取大盘当天上涨和下跌信息
    df['last_close'] = df['close'] / (1 + df['pct_chg'] / 100)
    df['pct_chg'] = 100 * (df['open'] - df['last_close']) / (df['last_close'])
    # 计算上涨和下跌家数，占比信息
    total_n = df.shape[0]
    up_n = df[df['pct_chg'] > 0].shape[0]  # 上涨家数
    even_n = df[df['pct_chg'] == 0].shape[0]  # 平盘家数
    down_n = df[df['pct_chg'] < 0].shape[0]  # 下跌家数
    up_r = up_n / total_n
    even_r = even_n / total_n
    down_r = down_n / total_n
    # 计算上涨和下跌家数
    one_three_up_n = df[(df['pct_chg'] >= 0) & (df['pct_chg'] < 3)].shape[0]  # 0-3上涨家数
    three_five_up_n = df[(df['pct_chg'] >= 3) & (df['pct_chg'] < 5)].shape[0]  # 0-3上涨家数
    five_eight_up_n = df[(df['pct_chg'] >= 5) & (df['pct_chg'] < 8)].shape[0]  # 0-3上涨家数
    one_three_down_n = df[(df['pct_chg'] >= -3) & (df['pct_chg'] < 0)].shape[0]  # 0-3上涨家数
    three_five_down_n = df[(df['pct_chg'] >= -5) & (df['pct_chg'] < -3)].shape[0]  # 0-3上涨家数
    five_eight_down_n = df[(df['pct_chg'] >= -8) & (df['pct_chg'] < -5)].shape[0]  # 0-3上涨家数
    one_three_up_r = one_three_up_n / total_n
    three_five_up_r = three_five_up_n / total_n
    five_eight_up_r = five_eight_up_n / total_n
    one_three_down_r = one_three_down_n / total_n
    three_five_down_r = three_five_down_n / total_n
    five_eight_down_r = five_eight_down_n / total_n
    # 获取涨停信息
    zt_n = df[df['pct_chg'] >= 9].shape[0]  # 跌停家数  # 涨停数
    dt_n = df[df['pct_chg'] <= -9].shape[0]  # 跌停家数
    data = {
        "up_n": [up_n],
        "even_n": [even_n],
        "down_n": [down_n],
        "up_r": [up_r],
        "even_r": [even_r],
        "down_r": [down_r],
        "one_three_up_n": [one_three_up_n],
        "three_five_up_n": [three_five_up_n],
        "five_eight_up_n": [five_eight_up_n],
        "one_three_up_r": [one_three_up_r],
        "three_five_up_r": [three_five_up_r],
        "five_eight_up_r": [five_eight_up_r],
        "one_three_down_r": [one_three_down_r],
        "three_five_down_r": [three_five_down_r],
        "five_eight_down_r": [five_eight_down_r],
        "zt_n": [zt_n],
        "dt_n": [dt_n]
    }
    return data


def calculate_stock_open_features(valid_codes, this_date, last_date):
    # 获得技术指标
    all_df_features = SqlUtil.get_batch_stock_tech_indicators_single_date(valid_codes, last_date)
    # 获取前一天的性能数据
    all_stock_data = SqlUtil.get_stock_data_batch_from_sql(valid_codes, this_date, this_date)
    all_stock_data.drop_duplicates(subset=['code'], inplace=True, keep='last')
    # 截取代码和开盘价信息，其他信息删除
    all_stock_data = all_stock_data[['code', 'open']].copy()
    all_stock_data.rename(columns={"open": "next_open"}, inplace=True)
    # 匹配该个股的性能数据和特征数据
    Utils.astype_before_merge(all_df_features, all_stock_data, cols=['code'])
    all_df_features = pd.merge(all_df_features, all_stock_data, on=['code'], how='inner')
    valid_cols = []
    for col in all_df_features.columns:
        if ('open' in col or 'close' in col or 'high' in col or 'low' in col) and (
                'ma' in col or 'hhv' in col or 'llv' in col or 'trend' in col):
            valid_cols.append(col)
    print('性能指标相关的计数器有：', len(valid_cols), '个。', valid_cols)
    result = collections.defaultdict(list)
    for code, df in all_df_features.groupby(['code']):
        result['code'].append(code)
        result['date'].append(last_date)
        for col in valid_cols:
            val = df[col].values[0]
            next_open = df['next_open'].values[0]
            result['next_open_' + col].append(round(next_open / val, 3))
    return result


def get_stock_concept_next_open_features(codes, stock_concept_dict, this_date):
    # 找到个股对应的板块信息
    result_df = pd.DataFrame()
    for code in codes:
        # 获取个股概念
        concept = stock_concept_dict.get(code, '')
        if not concept:
            print('找不到对应的概念', code, concept, this_date)
            continue
        # 根据今日热点概念，获取成分股
        bool = stock_concept_df_wencai['concept'] == concept
        valid_codes = list(set(stock_concept_df_wencai[bool]['code'].values.tolist()))
        # 获取这些成分股的个股数据
        all_data = SqlUtil.get_stock_data_batch_from_sql(codes=valid_codes, beg=this_date, end=this_date)
        # 计算特征：
        if all_data.empty:
            print('当前个股所属概念找不到其他股票信息：', concept, code, this_date)
            continue
        # 获取这些个股的特征
        concept_features = get_stock_and_dapan_open_features(all_data)
        # 获取当前个股概念下涨停个股的第二天开盘表现
        other_codes = sacs[this_date][concept]['codes']
        all_data = SqlUtil.get_stock_data_batch_from_sql(codes=other_codes, beg=this_date, end=this_date)
        if all_data.empty:
            continue
        codes_features = get_stock_and_dapan_open_features(all_data)
        for key, val in codes_features.items():
            concept_features['zt_codes_' + key] = val
        concept_features['code'] = [code]
        concept_features['date'] = [this_date]
        result_df = pd.concat([result_df, pd.DataFrame(concept_features)])
    return result_df


def make_df_for_open_features(df, type):
    rename_cols = {}
    for col in df.columns:
        if col == 'code' or col == 'date':
            continue
        else:
            if 'next_open' in col:
                continue
            else:
                rename_cols[col] = type + '_next_open_' + col
    df.rename(columns=rename_cols, inplace=True)
    return df


def update_stock_open_features():
    df = pd.read_csv(r'../StaticsData/concept_features/stock_concept_basic_features.csv', dtype={"code": str})
    df.sort_values(by=['date'], inplace=True)
    for date, tmp in df.groupby(['date']):
        cons = set(tmp['concept'].values.tolist())
        print('日期：{} 具有概念数{}'.format(date, len(cons)))
        # 获取当天候选个股标的
        valid_codes = tmp['code'].values.tolist()
        next_date = all_dates[all_dates.index(date) + 1]
        # 获取下一天的整个大盘的开盘特征特征
        df = SqlUtil.get_stock_data_all_by_date(next_date)
        dapan_open_features = get_stock_and_dapan_open_features(df)
        dapan_open_features['date'] = [date]
        dapan_open_df = make_df_for_open_features(pd.DataFrame(dapan_open_features), type='dapan')
        # 获取每个个股的开盘特征,使用开盘价计算与其他所有特征价格的比值
        stock_open_features = calculate_stock_open_features(valid_codes, this_date=next_date, last_date=date)
        stock_open_df = make_df_for_open_features(pd.DataFrame(stock_open_features), type=None)
        # 获取个股对应板块的特征，其中的特征情况和大盘特征相同
        stock_concept_dict = dict(zip(tmp['code'].values.tolist(), tmp['concept'].values.tolist()))
        concepte_features_df = get_stock_concept_next_open_features(valid_codes, stock_concept_dict, next_date)
        if concepte_features_df.empty:
            continue
        concepte_features_df = make_df_for_open_features(concepte_features_df, type='concept')
        # 计算个股的龙虎榜特征

        # 将全部特征合并，作为最终的开盘特征
        final_df = pd.merge(stock_open_df, dapan_open_df, on=['date'], how='left')
        print('stock_open_df shape is:', stock_open_df.shape, 'dapan_open_df shape is:', dapan_open_df.shape)
        merged_all_feature_df = pd.merge(final_df, concepte_features_df, on=['code', 'date'], how='inner')
        print('final df shape is:', final_df.shape, 'concept df shape is:', concepte_features_df.shape)
        merged_all_feature_df.to_csv(os.path.join(save_dir, '{}.csv'.format(date)), index=False)


if __name__ == '__main__':
    concept_file_name = r'D:\LearningProgram\trendStrategySystem\StaticsData\concept_features\stock_concepts_wencai.csv'
    save_dir = r'D:\LearningProgram\trendStrategySystem\StaticsData\open_features'
    all_dates = SqlUtil.get_exist_zt_dates()
    stock_concept_df_wencai = pd.read_csv(concept_file_name, dtype={'code': str})
    update_stock_open_features()

# -*- encoding: utf-8 -*-
# user:LWM
import collections
import os.path

import SqlUtil, DataUtil, DateUtil, Utils
from tqdm import tqdm
import pandas as pd
from common.Const import Const

"""
构造训练数据，将全部的特征进行组合
技术面特征
    1、stock_tech_indicators : 包含股票的均线和MACD等其他技术指标
    
基本面特征
    1、stock_basic_info: 包含市盈率等基本面特征，这里面需要再次构造截面特征

大盘特征 
    1、dapan_tech_indicators ：包含大盘基本面特征和涨跌家数等
    2、dapan_data: 大盘的基本数据，需要构造成交量特征
    3、stock_dapan_feature：大盘技术面特征 。包含涨停数据特征
    
龙虎榜特征
    1、龙虎榜基本特征stock_lhb_featue
    2、净买入和净卖出额
    3、买一占比。卖1占比
    4、是否有机构买入，机构占比（金额/总成交）
    5、是否有机构卖出，卖出占比（金额/总成交）
    6、买入是否有东财西藏拉萨营业部
    7、卖入是否有东财西藏拉萨营业部
    8、买一的胜率
    9、卖一的胜率
    10、买一金额是否超过卖一，占比多少
    11、总买入是否大于总卖出
    12、买方机构数-卖方机构数
    13、买方和卖方机构数
    14、

开盘特征
    1、包含板块开盘特征
    2、个股开盘特征、
    3、大盘开盘特征


板块特征：
    1、stock_concept_basic_features.csv
    2、stock_concept_features 里面的特征
    3、额外计算price特征和vol特征的变化率信息


"""

sacs_file_name = r'D:\LearningProgram\trendStrategySystem\StaticsData\sacs.json'
sacs = Utils.read_json(sacs_file_name)
sacs_dates = list(sacs.keys())
sacs_dates = [x for x in sacs_dates if x > '2020-01-01']

stock_concept_feature_df = pd.read_csv(r'../StaticsData/concept_features/stock_concept_features.csv',
                                       dtype={"code": str, "date": str})
dapan_concept_vol_features_df = pd.read_csv(r'../StaticsData/concept_features/concept_dapan_vol_ratio_features.csv',
                                            dtype={"code": str, "date": str})
stock_concept_basic_feature_df = pd.read_csv(r'../StaticsData/concept_features/stock_concept_basic_features.csv',
                                             dtype={"code": str, "date": str})


def rename_column_name_for_feature(df, excep_cols, type):
    rename = {}
    for col in df.columns:
        if col in excep_cols:
            continue
        else:
            rename[col] = type + "_" + col
    df.rename(columns=rename, inplace=True)
    return df


def check_index(x):
    if x[0] == '6':
        return 0
    elif x[0] == '0':
        return 1
    else:
        return 2


def create_train_features():
    lhb_youzi_path = '../StaticsData/lhb_data5'
    df_controller = pd.read_csv('../StaticsData/stock_controller.csv', dtype={'code': str})
    for date in sacs_dates:
        if os.path.exists('../StaticsData/train_data/{}.csv'.format(date)):
            continue

        # 获取当天涨停个股数据
        zt_df = SqlUtil.get_zt_stock_data_by_date(date)
        valid_codes = zt_df['code'].tolist()
        valid_codes = list(set(valid_codes))
        print(date, len(valid_codes))

        # 获取技术面特征
        df1 = SqlUtil.get_batch_stock_tech_indicators_single_date(valid_codes, date)
        print("df1.shape:", df1.shape)

        # 获取基本面数据，里面需要增加是否为国资委这个特征
        df2 = SqlUtil.get_batch_stock_data_basic_info(valid_codes, date)
        df2 = pd.merge(df2, df_controller[['code', 'real_controller']], on=['code'], how='left')
        df2['is_guozi'] = df2['real_controller'].apply(lambda x: 1 if str(x).__contains__("国有") else 0)
        print("df2.shape:", df2.shape)

        df1, df2 = Utils.astype_before_merge(df1, df2, ['code', 'date'])
        final_df = pd.merge(df1, df2, how='inner', on=['code', 'date'])
        print("final_df.shape:", final_df.shape)

        # 获取大盘特征，大盘技术特征，主要是主板的。这里暂时忽略创业板和其他
        df3 = SqlUtil.get_dapan_tech_indicators(code='sh000001', date=date)
        df3 = rename_column_name_for_feature(df3, ['date'], type='dapan')
        print("df3.shape:", df3.shape)

        # todo  增加个股所在主板或者创业板成交量信息和总的成交量信息
        final_df['idx_type'] = final_df[['code']].apply(lambda x: check_index(x))

        final_df, df3 = Utils.astype_before_merge(final_df, df3, ['date'])
        final_df = pd.merge(final_df, df3, on=['date'], how='left')
        print("final_df.shape:", final_df.shape)

        # 获取大盘涨停特征
        df4 = SqlUtil.get_zt_dapan_feature(date)
        print("df4.shape:", df4.shape)

        final_df, df4 = Utils.astype_before_merge(final_df, df4, ['date'])
        final_df = pd.merge(final_df, df4, on=['date'], how='left')
        print("final_df.shape:", final_df.shape)

        # 获取概念特征
        # (1) 概念特征  date,concept (2)个股概念特征code,date,concept, (3)概念大盘特征 date,concept
        this_concept_basic = stock_concept_basic_feature_df[stock_concept_basic_feature_df['date'] == date]
        this_concept_basic.drop(['total'], axis=1, inplace=True)
        this_concept_feature = stock_concept_feature_df[stock_concept_feature_df['date'] == date]
        this_dapan_feature = dapan_concept_vol_features_df[dapan_concept_vol_features_df['date'] == date]
        df5 = pd.merge(this_concept_basic, this_concept_feature, on=['date', 'concept'], how='left')
        df6 = pd.merge(df5, this_dapan_feature, on=['date', 'concept'], how='left')
        final_df = pd.merge(final_df, df6, on=['date', 'code'], how='left')
        print("final_df.shape:", final_df.shape)

        # 获取龙虎榜特征
        lhb_file_name = '../StaticsData/lhb_data3/{}.csv'.format(date)
        lhb_basic_feature_df = pd.read_csv(lhb_file_name, encoding='utf-8', dtype={"date": str, "code": str})
        lhb_youzi_filename = os.path.join(lhb_youzi_path, '{}.csv'.format(date))
        lhb_youzi_feature_df = pd.read_csv(lhb_youzi_filename, encoding='utf-8', dtype={"date": str, "code": str})
        this_lhb_youzi_feature_df = lhb_youzi_feature_df[lhb_youzi_feature_df['date'] == date].copy()
        df7 = pd.merge(lhb_basic_feature_df, this_lhb_youzi_feature_df, on=['date', 'code'], how='left')
        final_df = pd.merge(final_df, df7, on=['date', 'code'], how='left')
        print("final_df.shape:", final_df.shape)

        # 获取大盘涨停数据
        final_df.to_csv('../StaticsData/train_data/{}.csv'.format(date), index=False)
        print('-----' * 20)


def create_train_label():
    file_path = '../StaticsData/train_data'
    for file_name in os.listdir(file_path):
        df = pd.read_csv(os.path.join(file_path, file_name), dtype={"code": str, "date": str})
        # 获取当天候选个股标的
        valid_codes = df['code'].values.tolist()
        date = df['date'].values[0]
        print(date, len(valid_codes))

        result = collections.defaultdict(list)
        # 获取未来三个交易日的数据
        end_date = DateUtil.get_n_days_after(date, 10)
        all_df = SqlUtil.get_stock_data_batch_10day_from_sql(valid_codes, date, end_date)
        for code, df in all_df.groupby(by=['code']):
            df.sort_values(by=['date'], ascending=True, inplace=True)
            df.drop_duplicates(subset=['date'], inplace=True, keep='last')
            df = df.iloc[:4, :].copy()  # 未来三天的情况
            today_df = df[df['date'] == date]
            if today_df.empty:
                continue
            # 如果次日开盘直接涨停则忽略该个股
            df['last_close'] = df['close'] / (1 + df['pct_chg'] / 100)
            if df.shape[0] < 4:
                continue

            if df['open'].values[1] > 1.098 * df['last_close'].values[1]:
                continue

            nxt_open = df['open'].values[1]  # 次日开盘价买入
            nxt_close = df['close'].values[1]  # 次日开盘价买入
            nxt_nxt_open = df['open'].values[2]  # 第三天开盘价
            nxt_nxt_close = df['close'].values[2]  # 第三天收盘价
            next_high_max = max(df['high'].values[2:])  # 未来三天最高价
            next_low_min = min(df['low'].values[2:])  # 未来三天最低价
            next_close_max = max(df['close'].values[2:])  # 未来
            next_open_max = max(df['open'].values[2:])

            result['date'].append(date)
            result['code'].append(code)
            result['nxt_open'].append(nxt_open)
            result['opens'].append(df['open'].values.tolist())
            result['next_high_max'].append(next_high_max)
            result['next_low_min'].append(next_low_min)
            result['next_close_max'].append(next_close_max)
            result['next_open_max'].append(next_open_max)
            # 不同类型下的样本正负标签
            result['label1'].append(1 if nxt_open < nxt_close else 0)  # 当天盈利为正样本
            result['label2'].append(1 if nxt_open < nxt_nxt_open else 0)  # 隔天开盘卖出盈利为正样本
            result['label3'].append(1 if nxt_open < next_high_max else 0)  # 隔天存在更高卖出价格为正样本
            result['label4'].append(1 if nxt_open < next_low_min else 0)  # 隔天存随便卖都能盈利为正样本
            result['label5'].append(1 if nxt_open < nxt_nxt_close else 0)  # 隔天收盘卖出盈利为正样本
            result['label6'].append(1 if nxt_open < nxt_nxt_close else 0)  # 隔天收盘卖出盈利为正样本

        pd.DataFrame(result).to_csv('../StaticsData/train_label/{}.csv'.format(date), index=False)


def merge_train_and_label():
    file_path = r'../StaticsData/train_data'
    label_path = r'../StaticsData/train_label'
    result = pd.DataFrame()
    for file_name in os.listdir(file_path):
        label = os.path.join(label_path, file_name)
        train = os.path.join(file_path, file_name)
        if os.path.exists(train) and os.path.exists(label):
            train_df = pd.read_csv(train)
            label_df = pd.read_csv(label)
            train_df, label_df = Utils.astype_before_merge(train_df, label_df, ['code', 'date'])
            df = pd.merge(train_df, label_df, on=['code', 'date'], how='inner')
            print(file_name, 'after merge shape is:', df.shape)
            result = pd.concat([result, df])
    result.to_csv(r'../StaticsData/train.csv', index=False)


if __name__ == '__main__':
    # create_train_features()
    create_train_label()
    # merge_train_and_label()

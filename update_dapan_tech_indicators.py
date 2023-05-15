# -*- encoding: utf-8 -*-
# user:LWM
import akshare
import pandas as pd

import SqlUtil, DataUtil, DateUtil, Utils, formula
from tqdm import tqdm

"""
更新大盘的技术指标 如MACD KDJ 等，存储于dapan_tech_indicators 数据库表中
1、均线特征 5,10,20,30,60,120,
2、趋势线特征 5,10,20,30,60
3、成交量均线信息
4、计算极值特征
5、计算换手率特征


"""


# 计算技术指标和基本均线信息
def update_dapan_basic_tech_indicators():
    codes = ['sh000001', 'sh000016', 'sh000300', 'sh000905', 'sz399001', 'sz399005', 'sz399006']
    for code in tqdm(codes):
        # 从数据库中获取基本历史数据 bar数据
        df = SqlUtil.get_single_dapan_data_all(code)
        df['date'] = df['date'].apply(lambda x: str(x))
        if df.shape[0] < 250:
            continue
        # 截取一段数据用于计算
        df_base = df[['date', 'code', 'open', 'close', 'high', 'low', 'volume']].iloc[-250:, :].copy()
        df_base.sort_values(by=['date'],ascending=True,inplace=True)
        df_base.index = list(range(df_base.shape[0]))

        # 计算基本均线信息
        df_base = calculate_ma_type_data(df_base)

        # 计算趋势线特征
        df_base = calculate_trend_data(df_base)

        # 计算成交量均线信息
        df_base = calculate_volume_ma_data(df, df_base)

        # 计算极值特征
        df_base = calculate_hhv_llv_data(df, df_base)

        # 计算偏离特征
        df_base1 = calculate_deviation_degree(df_base, 'open')
        df_base2 = calculate_deviation_degree(df_base, 'high')
        df_base3 = calculate_deviation_degree(df_base, 'close')
        df_base4 = calculate_deviation_degree(df_base, 'low')
        columns = list(df_base.columns) + list(df_base1.columns) + list(df_base2.columns) + list(
            df_base3.columns) + list(df_base4.columns)
        df_base = pd.concat([df_base, df_base1, df_base2, df_base3, df_base4], ignore_index=True, axis=1)
        df_base.columns = columns

        # 计算macd kdj skdj bbi等技术指标数据
        macd = formula.MACD(df['close'], FAST=9, SLOW=26, MID=12)
        kdj = formula.KDJ(df, 9, 3, 3)
        skdj = formula.SKDJ(df, 9, 3)
        bbi = formula.BBI(df, 3, 6, 12, 24)
        boll = formula.BOLL(df, 20)
        eoc = formula.ROC(df, 12, 6)
        mtm = formula.MTM(df, 6, 12)

        # 汇总全部特征数据并更新列名
        columns = list(df_base.columns) + list(macd.columns) + list(kdj.columns) + list(skdj.columns) + list(
            bbi.columns) + list(boll.columns) + list(eoc.columns) + list(mtm.columns)
        df_base = pd.concat([df_base, macd, kdj, skdj, bbi, boll, eoc, mtm], axis=1, ignore_index=True)
        df_base.columns = columns
        df_base.dropna(inplace=True)
        print(df_base.shape)

        # 获取数据库中已经存在的日期特征数据
        dapan_df_exist = SqlUtil.get_all_dapan_tech_indicators(code)
        exist_dates = dapan_df_exist['date'].values.tolist()
        exist_dates = [str(x) for x in exist_dates]
        df_base = df_base[~df_base['date'].isin(exist_dates)]

        # 插入新数据
        SqlUtil.insert_sql(df_base, 'dapan_tech_indicators')


def calculate_ma_type_data(df):
    df = Utils.get_stock_ma_data(df, type='open', lines=['ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120'])
    df = Utils.get_stock_ma_data(df, type='close', lines=['ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120'])
    df = Utils.get_stock_ma_data(df, type='high', lines=['ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120'])
    df = Utils.get_stock_ma_data(df, type='low', lines=['ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120'])
    df = Utils.get_stock_ma_data(df, type='volume', lines=['ma5', 'ma10', 'ma20', 'ma30', 'ma60', 'ma120'])
    return df


def calculate_volume_ma_data(df, df_base):
    ma5 = formula.MA(df['volume'], 5)
    ma10 = formula.MA(df['volume'], 10)
    ma20 = formula.MA(df['volume'], 20)
    ma30 = formula.MA(df['volume'], 30)
    ma60 = formula.MA(df['volume'], 60)
    df_base['v_ma5'] = ma5.copy()
    df_base['v_ma10'] = ma10.copy()
    df_base['v_ma20'] = ma20.copy()
    df_base['v_ma30'] = ma30.copy()
    df_base['v_ma60'] = ma60.copy()
    return df_base


def calculate_trend_data(df):
    # 计算趋势线信息
    base = ['ma5', 'ma10', 'ma20', 'ma30', 'ma60']
    result = pd.DataFrame()
    new_col = ['code', 'date']
    for idx, row in df.iterrows():
        if idx < 120:
            continue
        for type in ['open', 'close', 'high', 'low']:
            lines = [type + "_" + x for x in base]
            for line in lines:
                last_data = df.loc[:idx - 1, line].values
                if line + '_trend' not in new_col:
                    new_col.append(line + '_trend')
                row = Utils.get_stock_ma_trend_data(row, last_data, line)
        result = pd.concat([result, pd.DataFrame(row).T])
    result, df = Utils.astype_before_merge(result[new_col], df, 'code')
    df = pd.merge(result, df, how='inner')
    return df


def calculate_hhv_llv_data(df, df_base):
    for type in ['open', 'close', 'low', 'high']:
        hhv5 = formula.HHV(df[type], 5)
        hhv10 = formula.HHV(df[type], 10)
        hhv30 = formula.HHV(df[type], 30)
        hhv60 = formula.HHV(df[type], 60)
        llv5 = formula.LLV(df[type], 5)
        llv10 = formula.LLV(df[type], 10)
        llv30 = formula.LLV(df[type], 30)
        llv60 = formula.LLV(df[type], 60)
        df_base[type + '_hhv_5'] = hhv5.copy()
        df_base[type + '_hhv_10'] = hhv10.copy()
        df_base[type + '_hhv_30'] = hhv30.copy()
        df_base[type + '_hhv_60'] = hhv60.copy()
        df_base[type + '_llv_5'] = llv5.copy()
        df_base[type + '_llv_10'] = llv10.copy()
        df_base[type + '_llv_30'] = llv30.copy()
        df_base[type + '_llv_60'] = llv60.copy()
    return df_base


def calculate_deviation_degree(df, col_name='open'):
    cols = [col for col in df.columns if ('hhv' in col or 'llv' in col or 'trend' in col) and 'deg' not in col]
    new_df_lst = []
    for col in cols:
        series = (df[col_name] / df[col]) - 1
        tmp = pd.DataFrame({col_name + '_' + col + "_deg": series}, index=df.index)
        new_df_lst.append(tmp)
    df = pd.concat(new_df_lst, axis=1)
    return df


if __name__ == '__main__':
    update_dapan_basic_tech_indicators()

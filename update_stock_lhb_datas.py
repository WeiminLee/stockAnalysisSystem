# -*- encoding: utf-8 -*-
# user:LWM
import collections
import datetime
import json
import os
import time

import pandas as pd

import DateUtil
import SqlUtil, Utils
from tqdm import tqdm
import numpy as np

from common import Const

"""
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
"""


def merge_all_lhb_data_to_date():
    file_path = '../StaticsData/lhb_data'
    file_dict = collections.defaultdict(list)
    for file_name in os.listdir(file_path):
        date, code = file_name.split("_")
        file_dict[date].append(os.path.join(file_path, file_name))
    for date, file_lst in tqdm(file_dict.items()):
        if os.path.exists('../StaticsData/lhb_data2/{}.csv'.format(date)):
            continue
        df = pd.DataFrame()
        for file in file_lst:
            pa, file_name = os.path.split(file)
            date, code = file_name[:-4].split("_")
            this_df = pd.read_csv(file)
            this_df['code'] = code
            this_df['date'] = date
            df = pd.concat([df, this_df])
        df.sort_values(by=['code'], inplace=True)
        df.to_csv('../StaticsData/lhb_data2/{}.csv'.format(date), index=False)


def get_jigou_buy_sell_info(tmp, type, name):
    buy_sell_tmp = tmp[tmp['type'].str.contains(type)]
    jigou_buy_sell_tmp = buy_sell_tmp[buy_sell_tmp['name'] == name]
    if jigou_buy_sell_tmp.empty:
        has_jigou = 0
        jigou_r = 0
        jigou_buy_sell_r = 0
    else:
        has_jigou = 1
        jigou_r = round(jigou_buy_sell_tmp.shape[0] / buy_sell_tmp.shape[0], 3)
        if type == '卖':
            jigou_buy_sell_r = round(sum(jigou_buy_sell_tmp['sell'].values) / sum(buy_sell_tmp['sell'].values), 3)
        else:
            jigou_buy_sell_r = round(sum(jigou_buy_sell_tmp['sell'].values) / sum(buy_sell_tmp['sell'].values), 3)
    return has_jigou, jigou_r, jigou_buy_sell_r


def get_jigou_buy_sell_feature(tmp, this_date_code_lhb_df, type, name):
    # 机构买入和卖出占总成交占比
    buy_sell_tmp = tmp[tmp['type'].str.contains(type)]
    jigou_buy_sell_tmp = buy_sell_tmp[buy_sell_tmp['name'] == name]
    buy_jigou_total = sum(jigou_buy_sell_tmp['buy'].values)
    buy_jigou_total_net_r = round(buy_jigou_total / this_date_code_lhb_df['buy_net'].values[0],
                                  3)  # 机构占净买入额占比
    buy_jigou_total_vol_r = round(buy_jigou_total / this_date_code_lhb_df['lhb_amount'].values[0], 3)  # 机构占总成交占比
    buy_jigou_total_mv_r = round(buy_jigou_total / this_date_code_lhb_df['mv_vol'].values[0], 3)  # 机构占总流通占比
    return buy_jigou_total_net_r, buy_jigou_total_vol_r, buy_jigou_total_mv_r


def get_lhb_basic_features_jigou_lasa():
    file_path = r'../StaticsData/lhb_data2'
    lhb_feature = pd.read_csv('../StaticsData/lhb_features/lhb.csv', dtype={"code": str,'date':str})
    lhb_feature = lhb_feature[list(Const.lhb_rename.keys())]
    lhb_feature[Const.lhb_cols] = lhb_feature[Const.lhb_cols].apply(lambda x: round(x / 10000, 3))

    for file_name in tqdm(os.listdir(file_path)):
        df = pd.read_csv(os.path.join(file_path, file_name), dtype={"code": str})
        date = file_name[:-4]
        if os.path.exists('../StaticsData/lhb_data3/{}.csv'.format(date)):
            continue
        this_lhb_df = lhb_feature[lhb_feature['date'] == date].copy()
        if this_lhb_df.empty:
            continue
        result = collections.defaultdict(list)
        for code, tmp in df.groupby(['code']):
            tmp.fillna(0, inplace=True)
            this_date_code_lhb_df = this_lhb_df[this_lhb_df['code'] == code]
            this_date_code_lhb_df.drop_duplicates(subset=['code'], inplace=True, keep='first')
            if this_date_code_lhb_df.empty:
                continue

            # 是否有机构买入，计算机构买入占比
            has_buy_jigou, buy_jigou_r, buy_jigou_buy_r = get_jigou_buy_sell_info(tmp, '买', '机构专用')
            # 是否有机构卖出，以及卖出占比
            has_sell_jigou, sell_jigou_r, sell_jigou_sell_r = get_jigou_buy_sell_info(tmp, '卖', '机构专用')

            # 机构买入和卖出占总成交占比
            # 机构占净买入额占比,机构占总成交占比,,机构占总流通占比
            buy_jigou_total_net_r, buy_jigou_total_vol_r, buy_jigou_total_mv_r = get_jigou_buy_sell_feature(tmp,
                                                                                                            this_date_code_lhb_df,
                                                                                                            'buy',
                                                                                                            '机构专用')
            sell_jigou_total_net_r, sell_jigou_total_vol_r, sell_jigou_total_mv_r = get_jigou_buy_sell_feature(tmp,
                                                                                                               this_date_code_lhb_df,
                                                                                                               'sell',
                                                                                                               '机构专用')

            # 是否有拉萨天团
            has_buy_lasa, buy_lasa_r, buy_lasa_buy_r = get_jigou_buy_sell_info(tmp, '买', '拉萨')
            has_sell_lasa, sell_lasa_r, sell_lasa_sell_r = get_jigou_buy_sell_info(tmp, '卖', '拉萨')
            buy_lasa_total_net_r, buy_lasa_total_vol_r, buy_lasa_total_mv_r = get_jigou_buy_sell_feature(tmp,
                                                                                                         this_date_code_lhb_df,
                                                                                                         'buy',
                                                                                                         '拉萨')

            sell_lasa_total_net_r, sell_lasa_total_vol_r, sell_lasa_total_mv_r = get_jigou_buy_sell_feature(tmp,
                                                                                                            this_date_code_lhb_df,
                                                                                                            'sell',
                                                                                                            '拉萨')
            result['code'].append(code)
            result['date'].append(date)
            result['has_buy_jigou'].append(has_buy_jigou)
            result['buy_jigou_r'].append(buy_jigou_r)
            result['buy_jigou_buy_r'] = buy_jigou_buy_r
            result['has_sell_jigou'].append(has_sell_jigou)
            result['sell_jigou_r'].append(sell_jigou_r)
            result['sell_jigou_sell_r'].append(sell_jigou_sell_r)
            result['buy_jigou_total_net_r'].append(buy_jigou_total_net_r)
            result['buy_jigou_total_vol_r'].append(buy_jigou_total_vol_r)
            result['buy_jigou_total_mv_r'].append(buy_jigou_total_mv_r)
            result['sell_jigou_total_net_r'].append(sell_jigou_total_net_r)
            result['sell_jigou_total_vol_r'].append(sell_jigou_total_vol_r)
            result['sell_jigou_total_mv_r'].append(sell_jigou_total_mv_r)
            result['has_buy_lasa'].append(has_buy_lasa)
            result['buy_lasa_r'].append(buy_lasa_r)
            result['buy_lasa_buy_r'].append(buy_lasa_buy_r)
            result['has_sell_lasa'].append(has_sell_lasa)
            result['sell_lasa_r'].append(sell_lasa_r)
            result['sell_lasa_sell_r'].append(sell_lasa_sell_r)
            result['buy_lasa_total_net_r'].append(buy_lasa_total_net_r)
            result['buy_lasa_total_vol_r'].append(buy_lasa_total_vol_r)
            result['buy_lasa_total_mv_r'].append(buy_lasa_total_mv_r)
            result['sell_lasa_total_net_r'].append(sell_lasa_total_net_r)
            result['sell_lasa_total_vol_r'].append(sell_lasa_total_vol_r)
            result['sell_lasa_total_mv_r'].append(sell_lasa_total_mv_r)

        lhb_df = pd.merge(pd.DataFrame(result), this_lhb_df, on=['code', 'date'], how='inner')
        lhb_df.to_csv('../StaticsData/lhb_data3/{}.csv'.format(date), index=False)


def get_up_down_ratio(df, all_stock_data_df):
    up_cnt, down_cnt = 0, 0
    for (date, code), tmp in df.groupby(['date', 'code']):
        # 获取当天和未来三天的性能数据
        end = DateUtil.get_n_days_after(date, 3)
        bool1 = all_stock_data_df['date'] >= date
        bool2 = all_stock_data_df['date'] <= end
        bool3 = all_stock_data_df['code'] == code
        this_code_date_df = all_stock_data_df[bool1 & bool2 & bool3].copy()
        if this_code_date_df.empty:
            continue
        pct_chg_lst = this_code_date_df['pct_chg'].values[1:]
        if len(pct_chg_lst) == len([x for x in pct_chg_lst if x > 0]):
            up_cnt += 1
        else:
            down_cnt += 1
    if up_cnt + down_cnt == 0:
        up_ratio = 0
    else:
        up_ratio = round(up_cnt / (up_cnt + down_cnt), 3)
    return up_cnt, down_cnt, up_ratio


def calculate_lhb_features_and_write(this_dates, all_lhb_df):
    # 对每一天开始计算
    for date in this_dates:
        if os.path.exists('../StaticsData/lhb_data4/{}.json'.format(date)):
            continue
        df = all_lhb_df[all_lhb_df['date'] == date]

        result = {}
        # 获取这一天的前三个月的数据
        start_date = DateUtil.get_n_days_before_from_beg(date, 3 * 30)
        # 获取每个游资在过去三个月的数据
        bool1 = all_lhb_df['date'] >= start_date
        bool2 = all_lhb_df['date'] <= date
        last_three_month_lhb_df = all_lhb_df[bool1 & bool2]
        if last_three_month_lhb_df.empty:
            continue
        youzi_lst = set(df['name'].values.tolist())
        sub_result = {}
        # 获取全部历史三个月的性能数据
        codes = list(set(last_three_month_lhb_df['code'].values.tolist()))
        start_date = DateUtil.get_n_days_before_from_beg(date, 3 * 30 + 10)
        end_date = DateUtil.get_n_days_after(date, 3)
        all_youzi_stock_data_df = SqlUtil.get_stock_data_batch_from_sql(codes, start_date, end_date)
        print('日期：', date, ' 柚子数量：', len(youzi_lst), ' 过去三个月股票数量：', len(codes), ' 数据大小:',
              all_youzi_stock_data_df.shape)
        for youzi in youzi_lst:
            this_result = {}
            # 获取柚子买入和卖出的出现次数和频率
            a = last_three_month_lhb_df['name'] == youzi  # 过去三个月出现过这个柚子的时间
            # 买入特征
            b = last_three_month_lhb_df['type'].str.contains("买")  # 这个柚子是出于买入状态
            buy_n = last_three_month_lhb_df[a & b].shape[0]  # 过去三个月出现该柚子的次数（买入次数）
            buy_n_r = round(2 * buy_n / last_three_month_lhb_df.shape[0], 3)  # 出现频次（频率）
            # 获取柚子买入和卖出的胜率
            this_youzi_df = last_three_month_lhb_df[a & b]
            if this_youzi_df.empty:
                buy_up_cnt, buy_down_cnt, buy_up_ratio = 0, 0, 0
            else:
                buy_up_cnt, buy_down_cnt, buy_up_ratio = get_up_down_ratio(this_youzi_df, all_youzi_stock_data_df)

            # 卖出特征
            b = last_three_month_lhb_df['type'].str.contains("卖")
            sell_n = last_three_month_lhb_df[a & b].shape[0]
            sell_n_r = round(2 * sell_n / last_three_month_lhb_df.shape[0], 3)
            # 获取柚子买入和卖出的胜率
            this_youzi_df = last_three_month_lhb_df[a & b]
            if this_youzi_df.empty:
                sell_up_cnt, sell_down_cnt, sell_up_ratio = 0, 0, 0
            else:
                sell_up_cnt, sell_down_cnt, sell_up_ratio = get_up_down_ratio(this_youzi_df, all_youzi_stock_data_df)

            # 记录数据
            this_result['buy_n'] = buy_n  # 买入次数
            this_result['buy_n_r'] = buy_n_r  # 买入次数占比
            this_result['sell_n'] = sell_n  # 卖出次数
            this_result['sell_n_r'] = sell_n_r  # 卖出次数占比
            this_result['buy_up_cnt'] = buy_up_cnt  # 买入后上涨次数
            this_result['buy_down_cnt'] = buy_down_cnt  # 买入后下跌次数
            this_result['buy_up_ratio'] = buy_up_ratio  # 买入上涨概率
            this_result['sell_up_cnt'] = sell_up_cnt  # 卖出上涨次数
            this_result['sell_down_cnt'] = sell_down_cnt  # 卖出下跌次数
            this_result['sell_up_ratio'] = sell_up_ratio  # 卖出后上涨概率
            sub_result[youzi] = this_result
        result[date] = sub_result
        save_name = '../StaticsData/lhb_data4/{}.json'.format(date)
        Utils.write_json(save_name, result)


def get_valid_dates_by_lhb_data2():
    # 1、获取个股的买入和卖出信息
    file_path = '../StaticsData/lhb_data2'
    all_lhb_df = pd.DataFrame()
    for file_name in tqdm(os.listdir(file_path)):
        df = pd.read_csv(os.path.join(file_path, file_name), dtype={"code": str})
        all_lhb_df = pd.concat([all_lhb_df, df])
    jigou_name_set = set(all_lhb_df['name'].values.tolist())
    all_lhb_df.sort_values(by=['date'], ascending=True, inplace=True)
    dates = list(set(all_lhb_df['date'].values.tolist()))
    valid_dates = [x for x in dates if not os.path.exists('../StaticsData/lhb_data4/{}.json'.format(x))]
    return jigou_name_set, valid_dates, all_lhb_df


if __name__ == '__main__':
    # 获取柚子相关的特征  第一步需要调用HttpService中的内容
    # todo get_stock_lhb_info.py
    # 合并全部的lhb基础数据
    merge_all_lhb_data_to_date()
    # 计算龙虎榜基础特征
    get_lhb_basic_features_jigou_lasa()
    # 计算龙虎榜中柚子的特征数据
    jigou_name_set, valid_dates, all_lhb_df = get_valid_dates_by_lhb_data2()
    print('剩余个数：', len(valid_dates))
    calculate_lhb_features_and_write(valid_dates, all_lhb_df)

# -*- encoding: utf-8 -*-
# user:LWM
"""
更新涨停特征和大盘特征
"""

import SqlUtil
from tqdm import tqdm
import pandas as pd


def calculate_dapan_zt_features_by_stock_data():
    all_stocks_df = SqlUtil.get_stock_data_all(code='000001')
    dates = all_stocks_df['date'].values.tolist()
    last_date = SqlUtil.last_date_dapan_zt_features()
    for idx, date in tqdm(enumerate(dates)):
        if idx < 1 or date < last_date:
            continue
        # 获取大盘当天上涨和下跌信息
        all_stocks_df = SqlUtil.get_stock_data_all_by_date(date)
        if all_stocks_df.empty:
            continue
        total_n = all_stocks_df.shape[0]
        up_n = all_stocks_df[all_stocks_df['pct_chg'] > 0].shape[0]  # 上涨家数
        even_n = all_stocks_df[all_stocks_df['pct_chg'] == 0].shape[0]  # 平盘家数
        down_n = all_stocks_df[all_stocks_df['pct_chg'] < 0].shape[0]  # 下跌家数
        up_r = up_n / total_n
        even_r = even_n / total_n
        down_r = down_n / total_n

        one_three_up_n = all_stocks_df[(all_stocks_df['pct_chg'] >= 0) & (all_stocks_df['pct_chg'] < 3)].shape[0]  # 0-3上涨家数
        three_five_up_n = all_stocks_df[(all_stocks_df['pct_chg'] >= 3) & (all_stocks_df['pct_chg'] < 5)].shape[0]  # 0-3上涨家数
        five_eight_up_n = all_stocks_df[(all_stocks_df['pct_chg'] >= 5) & (all_stocks_df['pct_chg'] < 8)].shape[0]  # 0-3上涨家数
        one_three_down_n = all_stocks_df[(all_stocks_df['pct_chg'] >= -3) & (all_stocks_df['pct_chg'] < 0)].shape[0]  # 0-3上涨家数
        three_five_down_n = all_stocks_df[(all_stocks_df['pct_chg'] >= -5) & (all_stocks_df['pct_chg'] < -3)].shape[0]  # 0-3上涨家数
        five_eight_down_n = all_stocks_df[(all_stocks_df['pct_chg'] >= -8) & (all_stocks_df['pct_chg'] < -5)].shape[0]  # 0-3上涨家数

        one_three_up_r = one_three_up_n / total_n
        three_five_up_r = three_five_up_n / total_n
        five_eight_up_r = five_eight_up_n / total_n

        one_three_down_r = one_three_down_n / total_n
        three_five_down_r = three_five_down_n / total_n
        five_eight_down_r = five_eight_down_n / total_n

        # 获取涨停信息
        print(date)
        zt_df = SqlUtil.get_zt_stock_data_by_date(date)
        if zt_df.empty:
            continue
        zt_n = zt_df.shape[0]  # 涨停数
        dt_n = all_stocks_df[all_stocks_df['pct_chg'] <= -9].shape[0]  # 跌停家数

        max_lb_count = max(zt_df['lb_count'].values)  # 最高板
        kb_n = zt_df[zt_df['fst_fb'] != zt_df['lst_fb']].shape[0]  # 炸板数
        kb_r = kb_n / zt_n  # 炸板率
        lb_r = zt_df[zt_df['lb_count'] > 1].shape[0] / zt_n  # 连板率

        yizi_b = zt_df[(zt_df['fst_fb'] <= '093000') & (zt_df['fst_fb'] == zt_df['lst_fb'])].shape[0]  # 一字板数量
        yizi_b_r = yizi_b / zt_n
        shouban_n = zt_df[zt_df['zt_count'] == '1/1'].shape[0]
        shouban_n_r = shouban_n / zt_n
        erban_n = zt_df[zt_df['zt_count'] == '2/2'].shape[0]
        erban_n_r = erban_n / zt_n
        sanban_n = zt_df[zt_df['zt_count'] == '3/3'].shape[0]
        sanban_n_r = sanban_n / zt_n
        siban_n = zt_df[zt_df['zt_count'] == '4/4'].shape[0]
        siban_n_r = siban_n / zt_n
        wuban_n = zt_n - shouban_n - erban_n - sanban_n - siban_n
        wuban_n_r = wuban_n / zt_n

        # 开盘涨停数
        zt_0900_n = zt_df[zt_df['fst_fb'] < '092800'].shape[0]
        zt_0900_r = zt_0900_n / zt_n
        zt_1000_n = zt_df[(zt_df['fst_fb'] >= '093000') & (zt_df['fst_fb'] <= '100000')].shape[0]
        zt_1000_r = zt_1000_n / zt_n
        zt_1130_n = zt_df[(zt_df['fst_fb'] > '100000') & (zt_df['fst_fb'] <= '113000')].shape[0]
        zt_1130_r = zt_1130_n / zt_n
        zt_1400_n = zt_df[(zt_df['fst_fb'] >= '130000') & (zt_df['fst_fb'] <= '140000')].shape[0]
        zt_1400_r = zt_1400_n / zt_n
        zt_1500_n = zt_df[(zt_df['fst_fb'] > '140000') & (zt_df['fst_fb'] <= '150000')].shape[0]
        zt_1500_r = zt_1500_n / zt_n

        # 获取昨天的涨停板信息
        last_day_zt_df = SqlUtil.get_zt_stock_data_by_date(dates[idx - 1])
        last_day_zt_df['code'] = last_day_zt_df['code'].astype(str)
        zt_df['code'] = zt_df['code'].astype(str)
        codes = last_day_zt_df[last_day_zt_df['lb_count'] == 1]['code'].values.tolist()
        df3 = zt_df[zt_df['code'].isin(codes)]
        one2two_r = df3.shape[0] / last_day_zt_df.shape[0]
        codes = last_day_zt_df[last_day_zt_df['lb_count'] == 2]['code'].values.tolist()
        df3 = zt_df[zt_df['code'].isin(codes)]
        two2three_r = df3.shape[0] / last_day_zt_df.shape[0]
        codes = last_day_zt_df[last_day_zt_df['lb_count'] == 3]['code'].values.tolist()
        df3 = zt_df[zt_df['code'].isin(codes)]
        three2four_r = df3.shape[0] / last_day_zt_df.shape[0]

        data = {
            'date': [date],
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
            "dt_n": [dt_n],
            "max_lb_count": [max_lb_count],
            "kb_n": [kb_n],
            "kb_r": [kb_r],
            "lb_r": [lb_r],
            "yizi_b": [yizi_b],
            "yizi_b_r": [yizi_b_r],
            "shouban_n": [shouban_n],
            "shouban_n_r": [shouban_n_r],
            "erban_n": [erban_n],
            "erban_n_r": [erban_n_r],
            "sanban_n": [sanban_n],
            "sanban_n_r": [sanban_n_r],
            "siban_n": [siban_n],
            "siban_n_r": [siban_n_r],
            "wuban_n": [wuban_n],
            "wuban_n_r": [wuban_n_r],
            "zt_0900_n": [zt_0900_n],
            "zt_0900_r": [zt_0900_r],
            "zt_1000_n": [zt_1000_n],
            "zt_1000_r": [zt_1000_r],
            "zt_1130_n": [zt_1130_n],
            "zt_1130_r": [zt_1130_r],
            "zt_1400_n": [zt_1400_n],
            "zt_1400_r": [zt_1400_r],
            "zt_1500_n": [zt_1500_n],
            "zt_1500_r": [zt_1500_r],
            "one2two_r": [one2two_r],
            "two2three_r": [two2three_r],
            "three2four_r": [three2four_r]}
        SqlUtil.insert_sql(pd.DataFrame(data), 'dapan_zt_features')


if __name__ == '__main__':
    calculate_dapan_zt_features_by_stock_data()

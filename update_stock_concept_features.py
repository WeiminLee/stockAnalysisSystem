# -*- encoding: utf-8 -*-
# user:LWM
import collections
import pandas as pd
import DateUtil
import SqlUtil, Utils
from tqdm import tqdm
import numpy as np


def get_price_features(df, type):
    features = dict()
    total_n = df.shape[0]
    up_n = df[df['pct_chg'] > 0].shape[0]  # 上涨家数
    even_n = df[df['pct_chg'] == 0].shape[0]  # 平盘家数
    down_n = df[df['pct_chg'] < 0].shape[0]  # 下跌家数
    df['total'] = df['amount'] / (df['pct_vol'] * 1000000)  # 总流通市值
    up_r = up_n / total_n
    even_r = even_n / total_n
    down_r = down_n / total_n
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
    features[type + '_up_n'] = [up_n]
    features[type + '_even_n'] = [even_n]
    features[type + '_down_n'] = [down_n]
    features[type + '_up_r'] = [up_r]
    features[type + '_even_r'] = [even_r]
    features[type + '_down_r'] = [down_r]
    features[type + '_one_three_up_n'] = [one_three_up_n]
    features[type + '_three_five_up_n'] = [three_five_up_n]
    features[type + '_five_eight_up_n'] = [five_eight_up_n]
    features[type + '_one_three_up_r'] = [one_three_up_r]
    features[type + '_three_five_up_r'] = [three_five_up_r]
    features[type + '_five_eight_up_r'] = [five_eight_up_r]
    features[type + '_one_three_down_r'] = [one_three_down_r]
    features[type + '_three_five_down_r'] = [three_five_down_r]
    features[type + '_five_eight_down_r'] = [five_eight_down_r]
    return features


def get_volumne_features(df, type):
    features = dict()
    pct_vol_mean = np.mean(df['pct_vol'].values.tolist())  # 平均换手率
    pct_vol_std = np.std(df['pct_vol'].values.tolist())  # 换手率标准差
    total_amount = np.sum(df['total'].values.tolist())  # 总流动盘大小
    features[type + '_pct_vol_mean'] = [pct_vol_mean]
    features[type + '_pct_vol_std'] = [pct_vol_std]
    features[type + '_total_amount'] = [total_amount]
    return features


def calculate_open_features(valid_codes, date):
    all_data = pd.DataFrame()
    while all_data.empty:
        date = DateUtil.get_n_days_after(date, 1)
        all_data = SqlUtil.get_stock_data_batch_from_sql(codes=valid_codes, beg=date, end=date)
    all_data['last_close'] = all_data['close'] / (1 + all_data['pct_chg'] / 100)
    all_data['pct_chg'] = 100 * (all_data['open'] - all_data['last_close']) / (all_data['last_close'])
    all_data['pct_chg'] = all_data['pct_chg'].astype(float)
    # 计算价格特征数据
    next_day_open_price_feature = get_price_features(all_data, 'next_day_open')
    return next_day_open_price_feature


def calculate_features_with_single_concept(concept, valid_codes, date):
    features = {}
    # 获取这些成分股的个股数据
    all_data = SqlUtil.get_stock_data_batch_from_sql(codes=valid_codes, beg=date, end=date)
    if all_data.empty:
        return {}
    print('-----', date, concept, all_data.shape)
    all_data['pct_chg'] = all_data['pct_chg'].astype(float)
    # 计算开盘特征
    next_day_open_price_feature = calculate_open_features(valid_codes, date)
    # 计算当天特征数据
    today_price_feature = get_price_features(all_data, 'today')
    today_vol_feature = get_volumne_features(all_data, 'today')
    # 计算昨天的特征数据
    this_date = all_dates[all_dates.index(date) - 1]
    all_data = SqlUtil.get_stock_data_batch_from_sql(codes=valid_codes, beg=this_date, end=this_date)
    print('-----', this_date, concept, all_data.shape)
    all_data['pct_chg'] = all_data['pct_chg'].astype(float)
    yestday_price_feature = get_price_features(all_data, 'yestday')
    yestday_vol_feature = get_volumne_features(all_data, 'yestday')
    # 计算两天前的特征数据
    this_date = all_dates[all_dates.index(date) - 2]
    all_data = SqlUtil.get_stock_data_batch_from_sql(codes=valid_codes, beg=this_date, end=this_date)
    print('-----', this_date, concept, all_data.shape)
    all_data['pct_chg'] = all_data['pct_chg'].astype(float)
    two_days_before_price_feature = get_price_features(all_data, 'two_days_before')
    two_days_before_vol_feature = get_volumne_features(all_data, 'two_days_before')

    features.update(next_day_open_price_feature)
    features.update(today_price_feature)
    features.update(today_vol_feature)
    features.update(yestday_price_feature)
    features.update(yestday_vol_feature)
    features.update(two_days_before_price_feature)
    features.update(two_days_before_vol_feature)
    # 数据入库
    return features


def calculate_other_features_with_index():
    for date, sace_info in tqdm(sacs.items()):
        for con, data in sace_info.items():
            if con in invalid_cons:
                continue


def update_concept_features():
    df = pd.read_csv(r'../StaticsData/concept_features/stock_concept_basic_features.csv', dtype={"code": str})
    save_name = r'../StaticsData/concept_features/stock_concept_features.csv'
    df.sort_values(by=['date'], inplace=True)
    for date, tmp in df.groupby(['date']):
        if all_dates.index(date) < 10:
            continue
        cons = set(tmp['concept'].values.tolist())
        print('日期：{} 具有概念数{}'.format(date, len(cons)))
        for concept in cons:
            # 根据今日热点概念，获取成分股
            valid_codes = list(set(stock_cons_df[stock_cons_df['concept'] == concept]['code'].values.tolist()))
            if len(valid_codes) == 0:
                continue
            # 计算价格和成交量特征
            features = calculate_features_with_single_concept(concept, valid_codes, date)
            # 计算当前概念下个股的特征
            valid_codes2 = sacs[date][concept]['codes']
            features2 = calculate_features_with_single_concept(concept, valid_codes2, date)
            # 更新feature的key名称
            new_features2 = {}
            for k, v in features2.items():
                new_features2[k + "_sub_zt"] = v
            features.update(new_features2)
            features['date'] = [date]
            features['concept'] = [concept]
            # 数据入库
            df = pd.DataFrame(features)
            Utils.write_csv_mode_a(df,save_name)


def get_last_n_days_show_time(sace_info, date,n):
    res = collections.defaultdict(int)
    for i in range(1, n+1):
        day = all_dates[all_dates.index(date) - i]
        for concept, data in sace_info.items():
            if concept in invalid_cons:
                continue
            if concept in sacs[day].keys():
                res[concept] += 1
    return res


def get_stock_basic_concept_feature(sacs):
    final_df = pd.DataFrame()
    for date, sace_info in tqdm(sacs.items()):
        df = SqlUtil.get_zt_stock_data_by_date(date)
        codes = df['code'].values.tolist()
        result = collections.defaultdict(list)
        for code in codes:
            flag = False
            for concept, data in sace_info.items():
                if concept in invalid_cons:
                    continue
                if code in data['codes']:
                    result['code'].append(code)
                    result['date'].append(date)
                    result['concept'].append(concept)
                    result['total'].append(data['total'])
                    result['amount'].append(data['amount'])
                    result['strength'].append(round(data['total'] / len(data['codes']), 3))
                    flag = True
                    break
            cnt = 0
            for concept, data in sace_info.items():
                if concept in invalid_cons:
                    continue
                if code in data['codes']:
                    cnt += 1
            if flag:
                result['con_num'].append(cnt)
        df = pd.DataFrame(result)
        if df.empty:
            print(date)
            continue
        max_total = max(df['total'])
        df['is_best_con'] = df['total'].apply(lambda x: 1 if x == max_total else 0)
        last_day = all_dates[all_dates.index(date) - 1]
        df['is_last_day_con'] = df['concept'].apply(lambda x: 1 if x in sacs[last_day].keys() else 0)
        last_2_day = all_dates[all_dates.index(date) - 2]
        df['is_last_2_days_con'] = df['concept'].apply(lambda x: 1 if x in sacs[last_2_day].keys() else 0)
        # 过去1周出现次数
        res = get_last_n_days_show_time(sace_info,date,n=3)
        df['last_3_day_times'] = df['concept'].apply(lambda x: res.get(x, 0))
        res = get_last_n_days_show_time(sace_info, date, n=5)
        df['last_5_day_times'] = df['concept'].apply(lambda x: res.get(x, 0))
        res = get_last_n_days_show_time(sace_info, date, n=10)
        df['last_10_day_times'] = df['concept'].apply(lambda x: res.get(x, 0))
        final_df = pd.concat([final_df, df])
    final_df.to_csv(r'../StaticsData/concept_features/stock_concept_basic_features.csv', index=False)


if __name__ == '__main__':
    invalid_cons = ['上市新股', '连板股', '公告', '其他', '次新', '业绩预增', '重组', '中标', '新股与次新股',
                    '核准制次新股', '次新股','高送转预期','可转债','非科创次新股','周期股','周期','北交所','复牌',
                    '创业板重组松绑','三季报增长','海天酱油添加剂','超跌','中报预增','三季报预增','中报增长']
    stock_cons_df = pd.read_csv(
        r'D:\LearningProgram\trendStrategySystem\StaticsData\concept_features\stock_concepts_wencai.csv', dtype=str)
    sacs_file_path = r'../StaticsData/sacs.json'
    sacs = Utils.read_json(sacs_file_path)
    all_dates = SqlUtil.get_exist_zt_dates()
    # get_stock_basic_concept_feature(sacs)
    update_concept_features()

    """
    板块特征
       1. 所属板块,很难唯一定位一个板块
          1. 所属概念个数
          2. 所属概念是否为昨日热点
          3. 所属概念是否为当前第一大概念
          4. 所属概念是否为当前成交量最大的概念
          5. 所属概念过去3、5、10天出现次数
          6. 成交额与所属概念成交额的占比
       3. 所属概念成交额
       4. 所属概况中的个股开盘特征
       4. 所属概况中的个股开盘特征
          1. 高开数占比
          2. 低开数占比
       8. 是否是概念龙一？
       9. 是否是概念最先涨停的个股
       10. 是否是概念最正宗个股-概念相关系数
       11. 是否是概念最强个股？快速封板、快速拉升、
       12. 是否首版一字
       13. 截至今日涨停数
    """

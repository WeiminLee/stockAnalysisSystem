# -*- encoding: utf-8 -*-
# user:LWM
import pandas as pd
from tqdm import tqdm
import SqlUtil, WuyUtil, Utils
import warnings
from pandas.core.common import SettingWithCopyWarning

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

sacs_file_name = r'D:\LearningProgram\trendStrategySystem\StaticsData\sacs.json'


def get_valid_dates_with_dapna_feature():
    # 获取全部交易日期信息
    df = SqlUtil.get_stock_data_all(code='000001')
    all_dates = df['date'].values.tolist()
    # 获取概念特征数据
    sacs = Utils.read_json(sacs_file_name)
    sacs_dates = list(sacs.keys())
    valid_dates = [x for x in all_dates if x not in sacs_dates]
    print("剩余未更新的时间为：", valid_dates)
    return valid_dates


def get_dapan_data_by_wuyang(valid_dates):
    sacs = {}
    for date in tqdm(valid_dates):
        sacs_info = WuyUtil.get_features_by_wuyang(date)
        if not sacs_info:
            sacs_info = WuyUtil.get_wuyang_features_by_xpath(date)
        if date not in sacs.keys():
            sacs[date] = sacs_info
    return sacs


def update_existed_dapan_features(sacs):
    valid_dates = get_valid_dates_with_dapna_feature()
    new_sacs = get_dapan_data_by_wuyang(valid_dates)
    # 更新sacs数据
    for date, item in new_sacs.items():
        if date not in sacs:
            sacs[date] = item
    Utils.write_json(sacs_file_name, sacs)


if __name__ == '__main__':
    sacs = Utils.read_json(sacs_file_name)
    update_existed_dapan_features(sacs=sacs)

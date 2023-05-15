# -*- encoding: utf-8 -*-
# user:LWM
import json

import scipy
from matplotlib.pylab import date2num
import pandas as pd
import time
import numpy as np
import os


def spearman_corr(x, y):
    return scipy.stats.spearmanr(x, y)[0]


def date_to_num(dates):
    num_time = []
    for date in dates:
        num_date = date2num(date)
        num_time.append(num_date)
    return num_time


def timestamp2timestr(timeStamp):
    timeArray = time.localtime(timeStamp)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    return otherStyleTime


def format_codes_from_cls_to_sql(codes):
    return [x[2:] for x in codes]


def astype_before_merge(df1, df2, cols):
    df1[cols] = df1[cols].astype(str).copy()
    df2[cols] = df2[cols].astype(str).copy()
    return df1, df2


def change_code_name(code):
    if code[0] == '0' or code[0] == '3':
        return 'sz' + code
    else:
        return 'sh' + code


def get_stock_ma_data(df, type='close', lines=['ma5', 'ma10', 'ma20', 'ma30', 'ma60']):
    for line in lines:
        n = int(line[2:])
        df[type + "_" + line] = df[type].rolling(n).mean()
    return df


# 计算趋势斜率并给出下一个预测值
def calculate_trend_data_prediction(data=[6.12, 5.65, 5.71, 5.66, 5.50]):
    x = list(range(len(data)))
    reg = np.polyfit(x, data, 1)
    return round(reg[0] * len(data) + reg[1], 3)


def get_stock_ma_trend_data(row, last_datas, line):
    reg = 3
    row[line + '_trend'] = calculate_trend_data_prediction(last_datas[-reg:])
    return row


def write_csv_mode_a(df: pd.DataFrame, file_name):
    if os.path.exists(file_name):
        df.to_csv(file_name, mode='a', index=False, header=False)
    else:
        df.to_csv(file_name, index=False)


def is_valid_code(code, name):
    if code[0] == '8' or code[:3] == '688' or ('ST' in name) or ('退' in name):
        return False
    if code[0] != '0' or code[0] != '6' or code[0] != '3':
        return False
    return True


def read_json(file_path):
    res = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        res = json.load(f)
    return res


def write_json(file_path, data):
    if os.path.exists(file_path):
        os.remove(file_path)
    with open(file_path, 'w', encoding='utf-8') as fp:
        fp.write(json.dumps(data))

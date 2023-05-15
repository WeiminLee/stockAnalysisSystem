# -*- encoding: utf-8 -*-
# user:LWM
import datetime
import efinance as ef
import akshare as ak
import pandas as pd
import os

import DateUtil
import SqlUtil
import Utils
from common.Const import Const


def get_all_exist_stock_code():
    codes = ef.stock.get_realtime_quotes()
    codes_, names_ = codes['股票代码'].values, codes['股票名称'].values
    codes, names = [], []
    for idx, code in enumerate(codes_):
        if not Utils.is_valid_code(code, names_[idx]):
            continue
        codes.append(code)
        names.append(names_[idx])
    return codes, names


def get_stock_data_bar(codes, beg='19000101', end: str = '20500101'):
    df = ef.stock.get_quote_history(codes, beg, end, klt=101)
    return df


def get_stock_data_1m(code):
    df = ef.stock.get_quote_history(code, klt=1)
    return df


def get_stock_data_batch_1m(codes):
    df = ef.stock.get_quote_history(codes, klt=1)
    return df


# 获取每日涨停数据
def get_ztb_codes_by_date():
    df = ak.stock_zt_pool_previous_em()
    return df


# # 个股市盈率等基本面指标
def get_basic_info_by_code(code='000421'):
    df = ak.stock_a_lg_indicator(symbol=code)
    return df


# 获取实际控制人信息
def get_real_control_type():
    df = ak.stock_hold_control_cninfo()
    return df


# 获取股东数量等信息
def get_stock_hold_num_info(date='20210630'):
    df = ak.stock_hold_num_cninfo(date=date)
    return df


def get_lhb_info(start_date, end_date):
    df = ef.stock.get_daily_billboard(start_date=start_date, end_date=end_date)
    df.rename(columns=Const.rename_lhb, inplace=True)
    return df


if __name__ == '__main__':
    today = DateUtil.get_today()
    df = get_lhb_info(start_date='2023-04-30', end_date=today)
    print(df)
    # SqlUtil.insert_sql(df,'stock_lhb_info')

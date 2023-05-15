# -*- encoding: utf-8 -*-
# user:LWM

import time
import requests
from common.Const import Const
import collections
import pandas as pd
import datetime
from lxml import etree


def short_date_str(date):
    date_str = []
    for txt in date.split("-"):
        if txt[0] == '0':
            date_str.append(txt[1:])
        else:
            date_str.append(txt)
    date = '-'.join(date_str)
    return date


def format_ten_days_df(resp):
    tenDays = resp['tenDays']
    result = []
    for item in tenDays[-23:]:
        if len(item) == 20:
            a = [item[i] for i in range(len(item)) if i % 2 == 0]
            b = [item[i] for i in range(len(item)) if i % 2 == 1]
            result.append(a)
            result.append(b)
        else:
            result.append(item)
    dapan_info = pd.DataFrame(columns=tenDays[0], data=result).T
    columns = ['up_num', 'down_num', 'total_zt', 'total_dt', 'lb_cnt',
               'yizi', 'shou_ban', 'shou_ban_r', 'er_ban', 'er_ban_r', 'san_ban', 'san_ban_r', 'si_ban',
               'si_ban_r', 'wu_ban', 'wuban_r', 'kp_zt', 'ten_zt', 'ten_zt_r', 'ele_zt', 'ele_zt_r',
               'one_zt', 'one_zt_r', 'two_zt', 'two_zt_r', 'kaiban_num', 'fbl', 'one_two', 'two_three',
               'three_four', 'lbl', 'lbl_yestoday', 'zt_amount', 'total_amount', 'sz_amount', 'cy_amont', 'kc_amount']
    dapan_info.columns = columns[-32:]
    dapan_info['date'] = dapan_info.index
    dapan_info['date'] = dapan_info['date'].apply(
        lambda x: str(datetime.datetime.strptime(x, "%Y-%m-%d")).split(' ')[0])
    dapan_info.index = list(range(dapan_info.shape[0]))
    return dapan_info


def get_sacs_info(resp_dic):
    sacs_info = {}
    for sac in resp_dic['sacs']:
        sac_name = sac['sac']
        amount = sac['tj'].split("总成交金额")[1][:-1]
        codes = [x['code'] for x in sac['datas']]
        sacs_info[sac_name] = {}
        total = len(codes)
        sacs_info[sac_name]['amount'] = float(amount)
        sacs_info[sac_name]['total'] = total
        sacs_info[sac_name]['codes'] = codes
    return sacs_info


def make_today_dapan_zt_features(resp, date):
    today_info = {}
    for k, v in resp['today'].items():
        today_info[k] = [v]
    today_df = pd.DataFrame(today_info)
    today_df['date'] = [date]
    return today_df


def get_lbg_feature(resp, date):
    result = collections.defaultdict(list)
    for item in resp['lbg']['datas']:
        if item['precent'] >= 9.9:
            result['date'].append(date)  # 日期
            result['code'].append(item['code'])  # 代码
            result['name'].append(item['name'])  # 名称
            result['zt_time'].append(item['zt_time'])  # 涨停时间
            result['fb_amount'].append(item['fb_amount'])  # 封板金额
            result['amount'].append(item['amount'])  # 成交量
            result['tor'].append(item['tor'])  # 换手率
            result['ltsz'].append(item['ltsz'])  # ？？？
            result['lb_count'].append(item['lb_count'])  # 连板数
    return pd.DataFrame(result)


def get_top_concepts_by_wuyang(date):
    tt = time.time()
    url = "http://www.wuylh.com/replayrobot/json/{0}p.json?v={1}".format(date, tt)
    resp = requests.get(url, headers=Const.header)
    resp_dic = resp.json()
    result_dict = collections.defaultdict(list)
    for sac in resp_dic['sacs']:
        datas = sac['datas']
        for data in datas:
            result_dict['date'].append(date)
            result_dict['concept'].append(sac['sac'])
            result_dict['code'].append(data['code'])
            result_dict['name'].append(data['name'])
            result_dict['sub_concept'].append(data['subject_deatal'].split("<br>")[0])
            result_dict['tj'].append(sac['tj'])
    return pd.DataFrame(result_dict)


def get_features_by_wuyang(date):
    url = "http://www.wuylh.com/replayrobot/json/{0}p.json?v={1}".format(date, time.time())
    resp = requests.get(url, headers=Const.header)
    if resp.status_code != 200:
        # 尝试从html中解析需要的信息
        return {}
    resp = resp.json()
    # 获取特征数据
    # lbg_df = get_lbg_feature(resp, date)
    # today_dapan_df = make_today_dapan_zt_features(resp, date)
    sacs_info = get_sacs_info(resp)
    # dapan_tendays_df = format_ten_days_df(resp)
    return sacs_info


def get_wuyang_features_by_xpath(date):
    date = short_date_str(date)
    url = 'http://www.wuylh.com/replayrobot/wylh{}p.html'.format(date)
    resp = requests.get(url, headers=Const.header)
    resp.encoding = '2312'
    xpath = etree.HTML(resp.text)
    if xpath is None:
        print('当前日期无数据', url)
        return False
    # 解析获取sacs
    sacs = parse_xpath_for_sacs(xpath)
    # 解析获取tendays_df
    dapan_tendays_df = parse_xpath_for_tendays(xpath)
    return sacs


def parse_xpath_for_tendays(xpath):
    tab_lst = xpath.xpath('//table[@id="rt2"]')
    th_lst = xpath.xpath('//table[@id="rt2"]/thead/tr/th')
    tr_lst = xpath.xpath('//table[@id="rt2"]/tbody/tr')
    dateTime = []
    for th in th_lst[1:]:
        dateTime.append(th.xpath("./b/a/text()")[0])
    total_data = []
    total_data.append(dateTime)
    for tr in tr_lst:
        this_data = []
        this_rate = []
        for td in tr.xpath('./td'):
            n1 = td.xpath("./text()")[0]
            if td.xpath("./span"):
                n1 = n1[:-1]
                n2 = td.xpath("./span/text()")[0]
                this_rate.append(n2)
            this_data.append(n1)
        total_data.append(this_data)
        if this_rate:
            total_data.append(this_rate)
    pass


def parse_xpath_for_sacs(xpath):
    sacs = {}
    div_lst = xpath.xpath('//div[@class="reporthead"]')
    concepts = []
    for div in div_lst:
        result = {}
        h2 = div.xpath('./h2')
        if h2:
            span = h2[0].xpath("./span")
            if span:
                span_txt = span[0].text
                h2_txt = h2[0].text
                result['total'] = int(span_txt.split('，')[0][:-1])
                result['amount'] = float(span_txt.split('金额')[1][:-1])
                concepts.append(h2_txt.replace(" ", ''))
                sacs[h2_txt.replace(" ", '')] = result
    div_lst = xpath.xpath('//div[@class="table-responsive lbindex"]')
    all_codes = []
    for idx, div in enumerate(div_lst):
        codes = []
        for tr in div.xpath('./table/tbody/tr'):
            code = tr.xpath('./td/text()')[1]
            codes.append(code)
        all_codes.append(codes)
    for idx, codes in enumerate(all_codes[:len(sacs)]):
        sacs[concepts[idx]]['codes'] = codes
    return sacs

if __name__ == '__main__':
    get_features_by_wuyang(date='2023-04-06')
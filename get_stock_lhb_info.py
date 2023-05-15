# -*- encoding: utf-8 -*-
# user:LWM
import os.path
from tqdm import tqdm
from selenium import webdriver
from lxml import etree

import DataUtil
import DateUtil
from common import Const
import requests
import pandas as pd
import collections


# 获取每天龙虎榜数据
def get_lhb(date):
    final_df = pd.DataFrame()
    url = "http://www.lhbvip.com/index/daylst/lst?tradedate={}".format(date)
    resp = requests.get(url, headers=Const.Const.lbg_header, timeout=30).json()
    for item in resp:
        detil_url = "http://www.lhbvip.com/index/daylst/item?stockcode={0}&tradedate={1}&leixing={2}".format(
            item['stockcode'], date, item['leixing'])
        detail_resp = requests.get(detil_url, headers=Const.Const.lbg_header, timeout=30).json()
        result_dict = collections.defaultdict(list)
        for sub in detail_resp:
            buy_mingcheng = []
            buy_buymoney = []
            buy_sellmoney = []
            buy_jinge = []
            for buy in sub['buylst']:
                buy_mingcheng.append(buy['mingcheng'])
                buy_buymoney.append(buy['buymoney'])
                buy_sellmoney.append(buy['sellmoney'])
                buy_jinge.append(buy['jinge'])
            sell_mingcheng = []
            sell_buymoney = []
            sell_sellmoney = []
            sell_jinge = []
            for sell in sub['selllst']:
                sell_mingcheng.append(sell['mingcheng'])
                sell_buymoney.append(sell['buymoney'])
                sell_sellmoney.append(sell['sellmoney'])
                sell_jinge.append(sell['jinge'])

            max_len = max(len(buy_buymoney), len(sell_buymoney))

            result_dict['code'] = [item['stockcode']] * max_len
            result_dict['date'] = [date] * max_len
            result_dict['buy'] = buy_mingcheng + ['*'] * (max_len - len(buy_buymoney))
            result_dict['buy_buymoney'] = buy_buymoney + [0] * (max_len - len(buy_buymoney))
            result_dict['buy_sellmoney'] = buy_sellmoney + [0] * (max_len - len(buy_buymoney))
            result_dict['buy_jinge'] = buy_jinge + [0] * (max_len - len(buy_buymoney))

            result_dict['sell'] = sell_mingcheng + ['*'] * (max_len - len(sell_buymoney))
            result_dict['sell_buymoney'] = sell_buymoney + [0] * (max_len - len(sell_buymoney))
            result_dict['sell_sellmoney'] = sell_sellmoney + [0] * (max_len - len(sell_buymoney))
            result_dict['sell_jinge'] = sell_jinge + [0] * (max_len - len(sell_buymoney))
        final_df = pd.concat([final_df, pd.DataFrame(result_dict)])
    print(final_df)
    # SqlUtil.insert_sql(final_df, 'stock_lhb')


def parse_resp_json(resp):
    stock_data = resp[0]['data'][0]
    buy_sell_info = resp[1]['data']
    num = len(buy_sell_info) // 2
    buy_sell = collections.defaultdict(list)
    for data in buy_sell_info:
        if data['mmlb'].__contains__('买'):
            buy_sell['buy_type'].append(data['mmlb'])
            buy_sell['buy_name'].append(data['zsmc'])
            buy_sell['buy_buy'].append(data['mrje'])
            buy_sell['buy_sell'].append(data['mcje'])
        else:
            buy_sell['sell_type'].append(data['mmlb'])
            buy_sell['sell_name'].append(data['zsmc'])
            buy_sell['sell_buy'].append(data['mrje'])
            buy_sell['sell_sell'].append(data['mcje'])
    buy_sell['code'] = [stock_data['zqjc'].split("(")[1][:-1]] * num
    buy_sell['date'] = [stock_data['dqrq']] * num
    buy_sell['reason'] = [stock_data['plyy']] * num
    return pd.DataFrame(buy_sell)


def get_lhb_stock_by_date(date):
    url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1842_xxpl_after&TABKEY=tab1&txtStart=2019-12-10&txtEnd={}&random=0.1336727390075243'.format(
        date)
    resp = requests.get(url, headers=Const.Const.lbg_header)
    resp = resp.json()
    print(resp)
    codes = []
    print(resp[0]['data'])
    for data in resp[0]['data']:
        codes.append(data['zqdm'])
    return codes


def get_lhb_detail_info():
    url = 'https://datacenter-web.eastmoney.com/api/data/v1/get?'
    params = {
        'callback': 'jQuery1123011395805718465635_1681139300916',
        'reportName': 'RPT_BILLBOARD_DAILYDETAILSBUY',
        'columns': 'ALL',
        'filter': "(TRADE_DATE='2023-04-06')(SECURITY_CODE='000021')",
        'pageNumber': 1,
        'pageSize': 50,
        'sortTypes': '-1',
        'sortColumns': 'BUY',
        'source': 'WEB',
        'client': 'WEB',
        '_': '1681139300918'
    }
    response = requests.get(url, params=params)
    text = response.text
    print(text)


def judge_xpath_result(ele):
    if len(ele) == 0:
        return None
    elif ele[0] == '-':
        return None
    else:
        return ele[0]


def parse_xpath_get_buy_sell(result, tab_ele, type):
    # 买入龙虎榜信息
    for i, tr_lst in enumerate(tab_ele.xpath('./tbody/tr')):
        td_lst = tr_lst.xpath('./td')
        if len(td_lst[1].xpath('./div/a/text()')) == 0:
            continue
        sc_name = td_lst[1].xpath('./div/a/text()')[0]
        if len(td_lst[1].xpath('./div/div/span/text()')) == 2:
            cnt, rate = td_lst[1].xpath('./div/div/span/text()')
        else:
            cnt = '0次'
            rate = '0%'

        buy = judge_xpath_result(td_lst[2].xpath('./span/text()'))
        buy = float(buy) if buy else buy
        buy_ratio = judge_xpath_result(td_lst[3].xpath('./text()'))
        buy_ratio = float(buy_ratio[:-1]) if buy_ratio else buy_ratio
        sell = judge_xpath_result(td_lst[4].xpath('./span/text()'))
        sell = float(sell) if sell else sell
        sell_ratio = judge_xpath_result(td_lst[5].xpath('./text()'))
        sell_ratio = float(sell_ratio[:-1]) if sell_ratio else sell_ratio
        net = float(td_lst[6].xpath('./span/text()')[0])

        result['name'].append(sc_name)
        result['type'].append(type + str(i + 1))
        result['count'].append(int(cnt[:-1]))
        result['up_r'].append(float(rate[:-1]))
        result['buy'].append(buy)
        result['buy_r'].append(buy_ratio)
        result['sell'].append(sell)
        result['sell_r'].append(sell_ratio)
        result['net'].append(net)
    return result


def update_lhb_data_csv():
    last_date = '2019-01-01'
    df = pd.DataFrame()
    if os.path.exists(lhb_file_name):
        df = pd.read_csv(lhb_file_name, encoding='utf-8', dtype={"date": str,"code":str})
        df.sort_values(by=['date'], ascending=False, inplace=True)
        last_date = df['date'].values[0]
    today = DateUtil.get_today()
    if last_date < today:
        tmp = DataUtil.get_lhb_info(last_date, today)
        df = pd.concat([df, tmp])
        if os.path.exists(lhb_file_name):
            os.remove(lhb_file_name)
        df.to_csv(lhb_file_name, index=False, encoding='utf-8')
    return df


if __name__ == '__main__':
    lhb_file_name = r'../StaticsData/lhb_features/lhb.csv'
    df = update_lhb_data_csv()
    df.sort_values(by=['date'], ascending=True, inplace=True)

    option = webdriver.ChromeOptions()
    option.add_argument('--headless')
    driver = webdriver.Chrome(r'D:\LearningProgram\chromedriver.exe', options=option)
    driver.create_options()

    while True:
        for (date, code), tmp in tqdm(df.groupby(['date', 'code'])):
            date = str(date).split(" ")[0]
            if os.path.exists('../StaticsData/lhb_data/{}_{}.csv'.format(date, code)):
                continue
            if code[:3] == '688':
                continue
            url = "https://data.eastmoney.com/stock/lhb,{},{}.html".format(date, code)
            driver.get(url)
            # time.sleep(random.random(2))
            html = driver.page_source
            html_obj = etree.HTML(html)
            tab_ele = html_obj.xpath(".//div[@class='sub-content']/table[@class='default_tab']")
            if len(tab_ele) != 2:
                continue
            result = collections.defaultdict(list)
            # 买入龙虎榜信息
            result = parse_xpath_get_buy_sell(result, tab_ele[0], '买')
            # 卖出龙虎榜信息
            result = parse_xpath_get_buy_sell(result, tab_ele[1], '卖')

            pd.DataFrame(result).to_csv('../StaticsData/lhb_data/{}_{}.csv'.format(date, code), index=False,
                                        encoding='utf-8')

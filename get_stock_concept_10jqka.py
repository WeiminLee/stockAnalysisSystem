# -*- encoding: utf-8 -*-
# user:LWM
import collections
from urllib.request import urlopen, Request
from HttpService import update_pg_ip_hosts
import requests
from bs4 import BeautifulSoup
from common.Const import Const
import pandas as pd
from tqdm import tqdm
import DataUtil, Utils


def fetch_stock_concept_from_10jqka(stock_url,proxy):
    stock_concepts = []
    try:
        resp = requests.get(stock_url,headers=Const.header,proxies=proxy,timeout=5)
        resp.encoding = 'gbk'
        html = resp.text
        bs = BeautifulSoup(html, 'html.parser',)
        for link in bs.find_all('td', {"class": "gnName"}):
            stock_concepts.append(link.text.replace(" ", '').replace('\n', '').replace('\t', ''))
        for link in bs.find_all('td', {"class": "gnStockList"}):
            stock_concepts.append(link.text.replace(" ", '').replace('\n', '').replace('\t', ''))
        return stock_concepts
    except:
        print(stock_url)
        return stock_concepts


def make_stock_url(stock: str):
    url = 'https://basic.10jqka.com.cn/{}/concept.html'.format(stock)
    return url


def update_stock_concepts_write_csv():
    all_code, all_name = DataUtil.get_all_exist_stock_code()
    for stock, stock_name in tqdm(list(zip(all_code, all_name))):
        if stock in exist_codes:
            continue
        stock_url = make_stock_url(stock)
        stock_concepts = []
        # 获取个股概念
        duration = 0
        proxies = ip.get_valid_proxy()
        while duration < 4:
            proxy = proxies.pop()
            stock_concepts = fetch_stock_concept_from_10jqka(stock_url,proxy)
            duration += 1
            if stock_concepts:
                break
        result = collections.defaultdict(list)
        for cons in stock_concepts:
            result['concept'].append(cons)
            result['code'].append(stock)
            result['name'].append(stock_name)
            # 存储实体
        # 存储数据
        df = pd.DataFrame(result)
        file_name = '../StaticsData/stock_concepts.csv'
        Utils.write_csv_mode_a(df, file_name)


if __name__ == '__main__':
    ip = update_pg_ip_hosts.IP()
    stock_cons_df = pd.read_csv(r'D:\LearningProgram\trendStrategySystem\StaticsData\stock_concepts.csv', dtype=str)
    exist_codes = list(set(stock_cons_df['code'].values.tolist()))
    update_stock_concepts_write_csv()

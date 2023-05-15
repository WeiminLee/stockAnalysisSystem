# -*- encoding: utf-8 -*-
# user:LWM
import collections
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import Utils, DataUtil
from common.Const import Const


def make_stock_url(stock: str):
    url = 'http://basic.10jqka.com.cn/{}/operate.html'.format(stock)
    return url


def fetch_stock_concept_from_10jqka(stock_url):
    MainBuss, ProductType, ProductName = '', [], []
    try:
        req = Request(url=stock_url, headers=Const.header)
        html = urlopen(req)
        bs = BeautifulSoup(html, 'html.parser')
        for link in bs.find_all('li'):
            txt = link.text
            txt = txt.replace(' ', '').replace('。', '').replace('\t', '').replace('\n', '')
            if '主营业务' in link.text:
                MainBuss = txt.split("：")[1]
            elif '产品类型' in link.text:
                ProductType = txt.split("：")[1].split('、')
            elif '产品名称' in link.text:
                ProductName = txt.split("：")[1].split('、')
        return MainBuss, ProductType, ProductName
    except:
        return MainBuss, ProductType, ProductName


def get_stock_main_products_and_type():
    all_code, all_name = DataUtil.get_all_exist_stock_code()
    for stock, stock_name in tqdm(list(zip(all_code, all_name))):
        stock_url = make_stock_url(stock)
        # 获取个股概念
        duration = 0
        mb, pt, pn = '', [], []
        while duration < 4:
            mb, pt, pn = fetch_stock_concept_from_10jqka(stock_url)
            if mb:
                break
            duration += 1
        # 存储数据
        result = collections.defaultdict(list)
        result['code'].append(stock)
        result['name'].append(stock_name)
        result['main_bussiness'].append(mb)
        result['product_type'].append(';'.join(pt))
        result['product_name'].append(';'.join(pn))

        df = pd.DataFrame(result)
        file_name = '../StaticsData/stock_products.csv'
        Utils.write_csv_mode_a(df, file_name)


if __name__ == '__main__':
    get_stock_main_products_and_type()

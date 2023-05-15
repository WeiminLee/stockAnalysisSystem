# -*- coding: utf-8 -*-
import collections
import datetime
import random
import threading
from lxml import etree
import requests
import pandas as pd
import os
import const
import util
import update_pg_ip_hosts
import guba_get_date0

base_url = 'https://guba.eastmoney.com'


def write_done_code_txt(code):
    with open('../StaticsData/url_done_codes.txt', mode='a') as fp:
        fp.write(code + '\r')


def write_resp_empty_code_txt(code):
    with open('../StaticsData/url_empty_codes.txt', mode='a') as fp:
        fp.write(code + '\r')


def write_resp_error_code_txt(code):
    with open('../StaticsData/url_error_codes.txt', mode='a') as fp:
        fp.write(code + '\r')


def read_codes_from_txt():
    with open('../StaticsData/url_done_codes.txt', 'r') as fp:
        codes = fp.readlines()
    with open('../StaticsData/url_empty_codes.txt', 'r') as fp:
        codes2 = fp.readlines()
    with open('../StaticsData/url_error_codes.txt', 'r') as fp:
        codes3 = fp.readlines()
    return set([x.replace('\n', '') for x in codes + codes2])


def write_url_data_for_code(code, i, proxies):
    url = "https://guba.eastmoney.com/list,{}_{}.html".format(code, i)
    status, resp = get_code_page_resp(url, proxies)
    if not status:
        return False, {}
    print('{4} page:{0},code:{1},url:{2},resp:{3}'.format(i, code, url, resp, str(datetime.datetime.now())[:19]))
    status, result, div_lst = parse_resp_with_xpath(resp, url, code, i)
    if not status:
        return False, result
    return True, result


def parse_resp_with_xpath(resp, url, code, i):
    try:
        xpath = etree.HTML(resp.text, etree.HTMLParser(encoding="utf-8"))
    except Exception as e:
        print('xpath 解析失败,', e)
        return False, {}, []
    if xpath is None:
        print('当前相应xpath为空，当前最大页码为：', url)
        return False, {}, []
    try:
        result, div_lst = parse_xpath_ele_result(code, url, i, xpath)
    except:
        return False, {}, []
    return True, result, div_lst


def parse_xpath_ele_result(code, url, i, xpath):
    div_lst = xpath.xpath('//div[contains(@class,"articleh normal_post")]')
    result = collections.defaultdict(list)
    for div in div_lst:
        span_lst = div.xpath('./span')
        if len(span_lst) == 5:
            read = span_lst[0].xpath('./text()')[0]
            common = span_lst[1].xpath('./text()')[0]
            title = ''.join(span_lst[2].xpath('.//text()'))
            reply = span_lst[4].xpath('./text()')[0]
            suburl = span_lst[3].xpath('./a/@href')[0][2:]
            result['code'].append(code)
            result['url'].append(url)
            result['index'].append(i)
            result['readnum'].append(read)
            result['commonnum'].append(common)
            result['title'].append(title)
            result['replttime'].append(reply)
            result['suburl'].append(suburl)
    return result, div_lst


def get_code_page_resp(url, proxies):
    resp = None
    while proxies:
        proxy = proxies.pop()
        try:
            resp = requests.get(url, headers=const.RequsetParams.gb_header, proxies=proxy, timeout=20)
        except Exception as e:
            continue
        if resp.status_code == 200:
            return True, resp
    if resp is None:
        print(str(datetime.datetime.now())[:19], '--获取url的resp失败', url)
        return False, None


def get_this_page_sub_urls(code, url, div_lst):
    sub_url = []
    for div_ele in div_lst:
        for a in div_ele.xpath('.//a/@href'):
            if 'news' in a and code in a:
                sub_url.append(base_url + a)
    if not sub_url:
        write_resp_empty_code_txt(code)
        print(str(datetime.datetime.now())[:19], '--获取数据完成，当前最大页码为：', url)
    return sub_url


def get_sub_url_reply_time(code, url, proxies):
    while proxies:
        proxy = proxies.pop()
        reply_time, reply_num, click_num = guba_get_date0.get_date_and_relynum_from_url(code, url, proxy)
        if not reply_time:
            continue
        if reply_time < '2020-01-01':
            write_done_code_txt(code)
            print(str(datetime.datetime.now())[:19], ' ', code, '--已满足最新日志需求，停止请求新的url连接')
            return False
        else:
            return True
    print(str(datetime.datetime.now())[:19], '--获取最新时间信息错误，停止请求新的url连接')
    return False


def get_data_for_single_code(code):
    count = 0
    file_name = os.path.join(guba_url_path, '{}.csv'.format(code))
    exist_urls = set()
    if os.path.exists(file_name):
        df = pd.read_csv(file_name)
        exist_urls = set(df['suburl'].values.tolist())
    result_df = pd.DataFrame()
    while count < 10000:
        # IP 队列中存在IP，那么从中取出一个IP进行使用
        while True:
            proxies = ip.get_valid_proxy()
            if proxies:
                break
        status, result = write_url_data_for_code(code, count, proxies)
        count += 1
        if not status:
            return result_df
        result_df = pd.concat([result_df, pd.DataFrame(result)])
        if not result_df.empty:
            new_urls = set(result_df['suburl'].values.tolist())
            if len(new_urls & exist_urls) > 0:
                print('停止请求新页面，当前页面为：', count)
                result_df = result_df[~result_df['suburl'].isin(exist_urls)]
                break
    return result_df


def get_guba_url_lst(codes):
    for code in codes:
        result_df = get_data_for_single_code(code)
        if result_df.empty:
            continue
        util.write_csv_mode_a(result_df, os.path.join(guba_url_path, '{}.csv'.format(code)))


def do_crawl_multi_process(all_codes):
    thread_lst = []
    if len(all_codes) > 6:
        thread_num = 6
    else:
        thread_num = 1
    step = len(all_codes) // thread_num
    for i in range(thread_num):
        codes = all_codes[i:(i + 1) * step]
        thread = threading.Thread(target=get_guba_url_lst, args=[codes])
        thread.start()
        thread_lst.append(thread)
    for t in thread_lst:
        t.join()


if __name__ == '__main__':
    guba_url_path = r'D:\LearningProgram\data\guba_url'
    ip = ip.IP()
    while True:
        codes = util.get_all_sql_exist_stock_code()
        do_crawl_multi_process(codes)

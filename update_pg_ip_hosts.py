# -*- encoding: utf-8 -*-
# user:LWM
import collections
import time
import pandas as pd
import requests
import random
import datetime
import SqlUtil


KEY = 'B8477C31'
PWD = '9D985BFA5E83'

KEY2 = '305BD1B4'
PWD2 = 'ABAE4E1CEFE4'


class IP():
    url = "https://proxy.qg.net/extract?Key={}&Num=5&AreaId=&Isp=&DataFormat=json&DataSeparator=&Detail=0&Pool=1".format(
        KEY)
    url2 = "https://proxy.qg.net/extract?Key={}&Num=5&AreaId=&Isp=&DataFormat=json&DataSeparator=&Detail=0&Pool=1".format(
        KEY2)

    def __init__(self):
        self.ip_lst = []

    def get_ips_from_api(self):
        resp = requests.get(self.url).json()
        ip_lst = []
        if resp['Code'] == 0:
            ip_lst = resp['Data']
        df = collections.defaultdict(list)
        for ip in ip_lst:
            df['ip'].append(ip['IP'])
            df['port'].append(ip['port'])
            df['deadline'].append(ip['deadline'])
            df['host'].append(KEY + ":" + ip['IP'] + ":" + str(ip['port']))
        # 获取第二个账号的IP信息
        resp2 = requests.get(self.url2).json()
        if resp2['Code'] == 0:
            ip_lst = resp2['Data']
        for ip in ip_lst:
            df['ip'].append(ip['IP'])
            df['port'].append(ip['port'])
            df['deadline'].append(ip['deadline'])
            df['host'].append(KEY2 + ':' + ip['IP'] + ":" + str(ip['port']))
        return pd.DataFrame(df)

    def get_valid_proxy(self):
        df = SqlUtil.get_ip_data()
        if not df.empty:
            hosts = random.choices(df['host'].values.tolist(), k=5)
            proxys = []
            for host in hosts:
                key, ip, port = host.split(':')
                proxy = self.make_dynamic_proxies(key, ip, port)
                proxys.append(proxy)
            return proxys
        return False

    def make_dynamic_proxies(self, key, ip, port):
        proxyAddr = "{}:{}".format(ip, port)
        authKey = key
        password = PWD if key == KEY else PWD2
        # 账密模式
        proxyUrl = "http://%(user)s:%(password)s@%(server)s" % {
            "user": authKey,
            "password": password,
            "server": proxyAddr,
        }
        proxies = {
            "http": proxyUrl,
            "https": proxyUrl,
        }
        return proxies


def delete_invalid_ip(df):
    remain_hosts = []
    invalid_hosts = []
    for idx, row in df.iterrows():
        deadline = row['deadline']
        host = row['host']
        now_t = datetime.datetime.now()
        deadline = datetime.datetime.strptime(deadline, '%Y-%m-%d %H:%M:%S')
        if (deadline - now_t).seconds < 5 or (deadline - now_t).seconds > 100:
            invalid_hosts.append(host)
        else:
            remain_hosts.append(host)
    SqlUtil.drop_ip_data(invalid_hosts)
    return remain_hosts


if __name__ == '__main__':
    ip = IP()
    while True:
        df1 = SqlUtil.get_ip_data()
        remain_hosts = delete_invalid_ip(df1)
        print('当前IP池原有{}个IP，'.format(df1.shape[0]), '剩余{}个依旧有效。'.format(len(remain_hosts)))
        df2 = ip.get_ips_from_api()
        df2 = df2[~df2['host'].isin(remain_hosts)]
        df2.drop_duplicates(subset=['host'], inplace=True)
        SqlUtil.insert_sql(df2, 'ip_data')
        time.sleep(5)

import collections
import json
import os.path
import re
import execjs
import pandas as pd
import requests
import requests.utils
import Utils

global hexin_v
global search

url = "http://www.iwencai.com/customized/chart/get-robot-data"
hexin_v = 'A_s9xKmFHF9LXCep1AsV4wG1jNRgUA9SCWTTBu241_oRTBXKdSCfohk0Y1T-'

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Referer": "http://www.iwencai.com/unifiedwap/result?w=20221030%E6%B6%A8%E5%81%9C",
    "cookie": f"v={hexin_v}",
}


def update_time(data, JS):  # 更新hexin-v
    global hexin_v
    token_time_url = requests.post(url=url, headers=headers, data=json.dumps(data)).text
    if 'status_code' in token_time_url:
        return None
    url_js = re.compile('<script src="(?P<jsurl>.*?)" type=', re.S)
    url_js_t = "http:" + url_js.search(token_time_url).group("jsurl")
    tt = requests.get(url_js_t).text
    token_time_text = tt[:tt.find(";") + 1]
    hexin_v = JS.call("rt.update")
    with open("token_time.txt", "w") as fa:
        fa.write(token_time_text + "\n" + hexin_v)
    print("已更新hexin-v与时间")


def get_time_and_hexin_v():
    global hexin_v
    with open("token_time.txt", "r") as toke:
        token_time = toke.readline()
        hexin_v = toke.readline()
    return token_time


def get_JS(token_time):
    with open("hexin-v_get.js", "r", encoding="utf-8") as f:
        js = f.read()
    JS = execjs.compile(token_time + "\n" + js)  # 读取时间拼接进入js代码中
    return JS


def create_request_data(search):
    data = {
        "question": f"{search}",
        "perpage": 50,
        "page": 1,
        "secondary_intent": "stock",
        "log_info": {"input_type": "typewrite"},
        "source": "Ths_iwencai_Xuangu",
        "version": "2.0",
        "query_area": "",
        "block_list": "",
        "add_info": {
            "urp": {
                "scene": 1,
                "company": 1,
                "business": 1
            },

            "contentType": "json",
            "searchInfo": True
        },
        "rsh": "Ths_iwencai_Xuangu_jgmqjaru6eknu2mk5ght2v9du9lbw763",
    }
    return data


def get_wencai_resp(data):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "Referer": "http://www.iwencai.com/unifiedwap/result?w=20221030%E6%B6%A8%E5%81%9C",
        "cookie": f"v={hexin_v}",
    }
    resp = requests.post(url=url, data=json.dumps(data), headers=headers)
    if "//192.168.201.240" in resp.text:
        token_time = get_time_and_hexin_v()
        JS = get_JS(token_time)
        update_time(data, JS)
        # 从新获取内容
        token_time = get_time_and_hexin_v()
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
            "Referer": "http://www.iwencai.com/unifiedwap/result?w=20221030%E6%B6%A8%E5%81%9C",
            "cookie": f"v={hexin_v}",
        }
        try:
            resp = requests.post(url=url, data=json.dumps(data), headers=headers)
        except:
            token_time = get_time_and_hexin_v()
            JS = get_JS(token_time)
            update_time(data, JS)
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
                "Content-Type": "application/json",
                "Referer": "http://www.iwencai.com/unifiedwap/result?w=20221030%E6%B6%A8%E5%81%9C",
                "cookie": f"v={hexin_v}",
            }
            resp = requests.post(url=url, data=json.dumps(data), headers=headers)
        return resp.json()
    else:
        return resp.json()


def parse_data_from_resp(resp,con):
    result = collections.defaultdict(list)
    codes = re.findall(r"('股票代码': '\d+)", str(resp))
    codes = [x.split(":")[1][2:] for x in codes]
    names = [code_info.get(x,None) for x in codes]
    print(len(codes),len(names))
    result['name'] = names
    result['code'] = codes
    result['corr'] = ['' for _ in range(len(codes))]
    result['concept'] = [con for _ in range(len(codes))]
    return result


if __name__ == '__main__':
    sacs = Utils.read_json(r'../StaticsData/sacs.json')
    df = pd.read_csv(r'../StaticsData/concept_features/stock_concepts_wencai.csv')
    df_info = pd.read_csv(r'../StaticsData/stock_products.csv',dtype={"code":str})
    df_info.index = df_info['code']
    code_info = df_info['name'].to_dict()
    all = []
    for date, item in sacs.items():
        for con in item:
            all.append(con)
    urls = []
    from tqdm import tqdm
    exist_cons = set(df['concept'].values.tolist())
    print('剩余：',len(set(all)-exist_cons))
    for con in tqdm(list(set(all))):
        if con in exist_cons:
            continue
        print(con)
        data = create_request_data(con)
        resp = get_wencai_resp(data)
        result = parse_data_from_resp(resp,con)
        # 存储
        if not os.path.exists(r'../StaticsData/concept_features/stock_concepts_wencai.csv'):
            pd.DataFrame(result).to_csv('../StaticsData/concept_features/stock_concepts_wencai.csv', index=False)
        else:
            pd.DataFrame(result).to_csv('../StaticsData/concept_features/stock_concepts_wencai.csv', index=False, mode='a', header=False)

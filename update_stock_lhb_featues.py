import collections
import json
import os
from tqdm import tqdm

import pandas as pd

lhb_data2_path = r'../StaticsData/lhb_data2'
lhb_data3_path = r'../StaticsData/lhb_data3'
lhb_data4_path = r'../StaticsData/lhb_data4'


def update_stock_lhb_features():
    for file_name in tqdm(os.listdir(lhb_data2_path)):
        if os.path.exists('../StaticsData/lhb_data5/{}'.format(file_name)):
            continue

        df2 = pd.read_csv(os.path.join(lhb_data2_path, file_name), dtype={"date": str, "code": str})
        # 获取每个股的买三和卖三
        stock_lhb_dict = collections.defaultdict(dict)
        for code, df in df2.groupby(['code']):
            info = {}
            for idx, row in df.iterrows():
                info[row['type']] = row['name']
            stock_lhb_dict[code] = info
        # 获取个股的基础数据
        if not os.path.exists(os.path.join(lhb_data3_path, file_name)): continue
        df3 = pd.read_csv(os.path.join(lhb_data3_path, file_name), dtype={"date": str, "code": str})
        # 删除重复的数据，有的个股可能在同一天存在两个龙虎榜，这里只保留一个
        df3.drop_duplicates(subset=['code', 'date'], inplace=True, keep='first')

        # 获取对应日期的柚子特征数据
        if not os.path.exists(os.path.join(lhb_data4_path, file_name.replace('.csv', '.json'))): continue
        with open(os.path.join(lhb_data4_path, file_name.replace('csv', 'json')), 'r') as fp:
            data4 = json.load(fp)

        # 更新特征到基础特征中去
        result = collections.defaultdict(list)
        for idx, row in df3.iterrows():
            code = row['code']
            date = row['date']
            youzi_dict = data4[date]
            cols = list(youzi_dict[list(youzi_dict.keys())[0]].keys())

            buy1 = stock_lhb_dict[code].get('买1', '*')
            buy2 = stock_lhb_dict[code].get('买2', '*')
            buy3 = stock_lhb_dict[code].get('买3', '*')

            sell1 = stock_lhb_dict[code].get('卖1', '*')
            sell2 = stock_lhb_dict[code].get('卖2', '*')
            sell3 = stock_lhb_dict[code].get('卖3', '*')

            if buy1 in youzi_dict.keys():
                for key, val in youzi_dict[buy1].items():
                    result['buy1_' + key].append(val)
            else:
                for col in cols:
                    result['buy1_' + col].append(None)
            if buy2 in youzi_dict:
                for key, val in youzi_dict[buy2].items():
                    result['buy2_' + key].append(val)
            else:
                for col in cols:
                    result['buy2_' + col].append(None)
            if buy3 in youzi_dict:
                for key, val in youzi_dict[buy3].items():
                    result['buy3_' + key].append(val)
            else:
                for col in cols:
                    result['buy3_' + col].append(None)
            if sell1 in youzi_dict:
                for key, val in youzi_dict[sell1].items():
                    result['sell1_' + key].append(val)
            else:
                for col in cols:
                    result['sell1_' + col].append(None)
            if sell2 in youzi_dict:
                for key, val in youzi_dict[sell2].items():
                    result['sell2_' + key].append(val)
            else:
                for col in cols:
                    result['sell2_' + col].append(None)
            if sell3 in youzi_dict:
                for key, val in youzi_dict[sell3].items():
                    result['sell3_' + key].append(val)
            else:
                for col in cols:
                    result['sell3_' + col].append(None)

            result['code'].append(code)
            result['date'].append(date)

        pd.DataFrame(result).to_csv('../StaticsData/lhb_data5/{}'.format(file_name), index=False)


if __name__ == '__main__':
    update_stock_lhb_features()
